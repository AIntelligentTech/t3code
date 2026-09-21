import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { writeFileStringAtomically } from "./atomicWrite.ts";
import type * as ServerConfig from "./config.ts";
import { formatHostForUrl, isWildcardHost } from "./startupAccess.ts";

export const PersistedServerRuntimeState = Schema.Struct({
  version: Schema.Literal(1),
  pid: Schema.Int,
  host: Schema.optional(Schema.String),
  port: Schema.Int,
  origin: Schema.String,
  // Present when the server fronts a dev web server (VITE_DEV_SERVER_URL).
  // Dev is single-origin: browsers must pair through this URL, not `origin`.
  devUrl: Schema.optional(Schema.String),
  startedAt: Schema.String,
  /**
   * Set when the boot-service launcher supervises this server. Lets a CLI
   * tell a service-managed server apart from one started by hand, which is
   * the difference between "restart the service" and "stop your terminal".
   */
  serviceManaged: Schema.optional(Schema.Boolean),
});
export type PersistedServerRuntimeState = typeof PersistedServerRuntimeState.Type;

export class ServerRuntimeStateError extends Schema.TaggedError<ServerRuntimeStateError>()(
  "ServerRuntimeStateError",
  {
    operation: Schema.Literals(["persist", "read", "decode", "clear"]),
    statePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to ${this.operation} server runtime state at ${this.statePath}.`;
  }
}

const decodePersistedServerRuntimeState = Schema.decodeUnknownEffect(
  Schema.fromJsonString(PersistedServerRuntimeState),
);

const runtimeOriginForConfig = (
  config: Pick<ServerConfig.ServerConfig["Service"], "host">,
  port: number,
): PersistedServerRuntimeState["origin"] => {
  const hostname =
    config.host && !isWildcardHost(config.host) ? formatHostForUrl(config.host) : "127.0.0.1";
  return `http://${hostname}:${port}`;
};

export const makePersistedServerRuntimeState = (input: {
  readonly config: Pick<ServerConfig.ServerConfig["Service"], "host" | "devUrl">;
  readonly port: number;
  readonly serviceManaged?: boolean;
}): Effect.Effect<PersistedServerRuntimeState> =>
  Effect.map(DateTime.now, (now) => ({
    version: 1,
    pid: process.pid,
    ...(input.config.host ? { host: input.config.host } : {}),
    port: input.port,
    origin: runtimeOriginForConfig(input.config, input.port),
    ...(input.config.devUrl ? { devUrl: input.config.devUrl.toString() } : {}),
    startedAt: DateTime.formatIso(now),
    ...(input.serviceManaged ? { serviceManaged: true } : {}),
  }));

export const persistServerRuntimeState = (input: {
  readonly path: string;
  readonly state: PersistedServerRuntimeState;
}) =>
  writeFileStringAtomically({
    filePath: input.path,
    contents: `${JSON.stringify(input.state)}\n`,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ServerRuntimeStateError({
          operation: "persist",
          statePath: input.path,
          cause,
        }),
    ),
  );

export const clearPersistedServerRuntimeState = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.remove(path, { force: true }).pipe(
      Effect.mapError(
        (cause) =>
          new ServerRuntimeStateError({
            operation: "clear",
            statePath: path,
            cause,
          }),
      ),
      Effect.catchTags({
        ServerRuntimeStateError: (error) =>
          Effect.logWarning(error.message).pipe(
            Effect.annotateLogs({
              operation: error.operation,
              statePath: error.statePath,
              cause: error,
            }),
          ),
      }),
    );
  });

/**
 * Report whether the pid recorded in a persisted runtime state is still
 * running. Signal 0 delivers nothing; it only reports whether the pid exists.
 * EPERM means it exists but belongs to another user, which still counts as
 * alive.
 */
export const isProcessAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error instanceof Error && "code" in error && error.code === "EPERM";
  }
};

export const readPersistedServerRuntimeState = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const raw = yield* fs.readFileString(path).pipe(
      Effect.matchEffect({
        onFailure: (cause) =>
          cause.reason._tag === "NotFound"
            ? Effect.succeed(Option.none<string>())
            : Effect.fail(
                new ServerRuntimeStateError({
                  operation: "read",
                  statePath: path,
                  cause,
                }),
              ),
        onSuccess: (contents) => Effect.succeed(Option.some(contents)),
      }),
    );
    if (Option.isNone(raw)) {
      return Option.none<PersistedServerRuntimeState>();
    }

    const trimmed = raw.value.trim();
    if (trimmed.length === 0) {
      return Option.none<PersistedServerRuntimeState>();
    }

    return yield* decodePersistedServerRuntimeState(trimmed).pipe(
      Effect.map(Option.some),
      Effect.mapError(
        (cause) =>
          new ServerRuntimeStateError({
            operation: "decode",
            statePath: path,
            cause,
          }),
      ),
    );
  }).pipe(
    Effect.catchTags({
      ServerRuntimeStateError: (error) =>
        Effect.logWarning(error.message).pipe(
          Effect.annotateLogs({
            operation: error.operation,
            statePath: error.statePath,
            cause: error,
          }),
          Effect.as(Option.none<PersistedServerRuntimeState>()),
        ),
    }),
  );

/**
 * Two `t3` servers against the same `--base-dir` restore the same session
 * store independently, and their command-side read models diverge silently:
 * a thread created on one is invisible to the other's command dispatcher
 * (see `apps/server/src/orchestration/Layers/OrchestrationEngine.ts`, which
 * hydrates its in-process read model once at boot and only replays events
 * dispatched through *that* process afterwards). There is no lock today —
 * a second `serve` on a free port starts happily against the same store.
 *
 * This error names what a caller needs to resolve the conflict without
 * digging: the pid to stop, and where the live server is already answering.
 */
export class ServerAlreadyRunningError extends Schema.TaggedError<ServerAlreadyRunningError>()(
  "ServerAlreadyRunningError",
  {
    statePath: Schema.String,
    state: PersistedServerRuntimeState,
  },
) {
  override get message(): string {
    return formatServerAlreadyRunningMessage(this);
  }
}

export const formatServerAlreadyRunningMessage = (input: {
  readonly statePath: string;
  readonly state: PersistedServerRuntimeState;
}): string =>
  `A t3 server is already running for this --base-dir (pid ${input.state.pid}, ` +
  `serving at ${input.state.origin}, started ${input.state.startedAt}). ` +
  `Stop it first, or point --base-dir at a different directory: two servers ` +
  `sharing one store diverge silently rather than failing loudly ` +
  `(runtime state read from ${input.statePath}).`;

/**
 * Refuse to continue when a DIFFERENT live server already holds this
 * base-dir's runtime-state file. Never called from inside a fresh process
 * that has not yet bound a port — call it before any listener is created,
 * so a refusal never leaves a half-started server behind.
 *
 * Two things this deliberately does NOT do, both load-bearing:
 * - It does not refuse on a file whose pid is dead. `clearPersistedServerRuntimeState`
 *   only runs on a clean shutdown, so a crashed or killed server leaves this
 *   file behind; treating a stale file as a lock would turn every crash into
 *   a permanent outage nobody could restart without manual cleanup.
 * - It does not refuse on the CURRENT process's own pid. A server restarting
 *   in place (same pid re-execing, or a supervisor rewriting the file after
 *   this check ran) must never be blocked by its own prior state.
 */
export const guardAgainstConcurrentServer = (statePath: string) =>
  Effect.gen(function* () {
    const existing = yield* readPersistedServerRuntimeState(statePath);
    if (Option.isNone(existing)) {
      return;
    }
    const state = existing.value;
    if (state.pid === process.pid || !isProcessAlive(state.pid)) {
      return;
    }
    return yield* new ServerAlreadyRunningError({ statePath, state });
  });
