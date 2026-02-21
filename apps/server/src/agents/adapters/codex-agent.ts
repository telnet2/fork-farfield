import fs from "node:fs";
import path from "node:path";
import {
  AppServerClient,
  AppServerRpcError,
  AppServerTransportError,
  CodexMonitorService,
  DesktopIpcClient,
  findLatestTurnParamsTemplate,
  reduceThreadStreamEvents,
  ThreadStreamReductionError,
  type SendRequestOptions
} from "@farfield/api";
import type { JsonRpcNotification, JsonRpcServerRequest } from "@farfield/api";
import {
  parseThreadStreamStateChangedBroadcast,
  parseUserInputResponsePayload,
  ProtocolValidationError,
  type IpcFrame,
  type IpcRequestFrame,
  type IpcResponseFrame
} from "@farfield/protocol";
import { logger } from "../../logger.js";
import { resolveOwnerClientId } from "../../thread-owner.js";
import type {
  AgentAdapter,
  AgentCapabilities,
  AgentCreateThreadInput,
  AgentCreateThreadResult,
  AgentInterruptInput,
  AgentListThreadsInput,
  AgentListThreadsResult,
  AgentReadThreadInput,
  AgentReadThreadResult,
  AgentSendMessageInput,
  AgentSetCollaborationModeInput,
  AgentSubmitUserInputInput,
  AgentThreadLiveState,
  AgentThreadStreamEvents
} from "../types.js";

export interface CodexAgentRuntimeState {
  appReady: boolean;
  ipcConnected: boolean;
  ipcInitialized: boolean;
  codexAvailable: boolean;
  lastError: string | null;
}

export interface CodexIpcFrameEvent {
  direction: "in" | "out";
  frame: IpcFrame;
  method: string;
  threadId: string | null;
}

export interface CodexAgentOptions {
  appExecutable: string;
  socketPath: string;
  workspaceDir: string;
  userAgent: string;
  reconnectDelayMs: number;
  onStateChange?: () => void;
}

const ANSI_ESCAPE_REGEX = /\u001B\[[0-?]*[ -/]*[@-~]/g;
const INVALID_STREAM_EVENTS_LOG_PATH = process.env["FARFIELD_INVALID_STREAM_LOG_PATH"] ??
  path.resolve(process.cwd(), "invalid-thread-stream-events.jsonl");

const APP_SERVER_NOTIFICATION_LIMIT = 400;

function extractThreadIdFromNotificationParams(params: unknown): string | null {
  if (!params || typeof params !== "object") {
    return null;
  }

  const record = params as Record<string, unknown>;
  const candidates = ["threadId", "conversationId", "thread_id"];
  for (const key of candidates) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return null;
}

function notificationToSyntheticFrame(notification: JsonRpcNotification): IpcFrame {
  return {
    type: "broadcast",
    method: notification.method,
    params: notification.params,
    sourceClientId: "app-server"
  };
}

export class CodexAgentAdapter implements AgentAdapter {
  public readonly id = "codex";
  public readonly label = "Codex";
  public readonly capabilities: AgentCapabilities = {
    canListModels: true,
    canListCollaborationModes: true,
    canSetCollaborationMode: true,
    canSubmitUserInput: true,
    canReadLiveState: true,
    canReadStreamEvents: true
  };

  private readonly appClient: AppServerClient;
  private readonly ipcClient: DesktopIpcClient;
  private readonly service: CodexMonitorService;
  private readonly onStateChange: (() => void) | null;
  private readonly reconnectDelayMs: number;

  private readonly threadOwnerById = new Map<string, string>();
  private readonly streamEventsByThreadId = new Map<string, IpcFrame[]>();
  private readonly appServerNotificationsByThreadId = new Map<string, JsonRpcNotification[]>();
  private readonly pendingServerRequests = new Map<number, JsonRpcServerRequest>();
  private readonly ipcFrameListeners = new Set<(event: CodexIpcFrameEvent) => void>();

  private runtimeState: CodexAgentRuntimeState = {
    appReady: false,
    ipcConnected: false,
    ipcInitialized: false,
    codexAvailable: true,
    lastError: null
  };

  private bootstrapInFlight: Promise<void> | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private started = false;

  public constructor(options: CodexAgentOptions) {
    this.onStateChange = options.onStateChange ?? null;
    this.reconnectDelayMs = options.reconnectDelayMs;

    this.appClient = new AppServerClient({
      executablePath: options.appExecutable,
      userAgent: options.userAgent,
      cwd: options.workspaceDir,
      onStderr: (line) => {
        const normalized = normalizeStderrLine(line);
        if (isKnownBenignAppServerStderr(normalized)) {
          logger.debug({ line: normalized }, "codex-app-server-stderr-ignored");
          return;
        }
        logger.error({ line: normalized }, "codex-app-server-stderr");
      }
    });

    this.ipcClient = new DesktopIpcClient({
      socketPath: options.socketPath
    });
    this.service = new CodexMonitorService(this.ipcClient);

    // Listen for app-server notifications (streaming events).
    this.appClient.onNotification((notification) => {
      this.handleAppServerNotification(notification);
    });

    // Listen for server-to-client requests (approval prompts, user input).
    this.appClient.onServerRequest((request) => {
      this.handleAppServerRequest(request);
    });

    this.ipcClient.onConnectionState((state) => {
      this.patchRuntimeState({
        ipcConnected: state.connected,
        ipcInitialized: state.connected ? this.runtimeState.ipcInitialized : false,
        ...(state.reason ? { lastError: state.reason } : {})
      });

      if (!state.connected) {
        this.scheduleIpcReconnect();
      } else if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
    });

    this.ipcClient.onFrame((frame) => {
      const threadId = extractThreadId(frame);
      const method = frame.type === "request" || frame.type === "broadcast"
        ? frame.method
        : frame.type === "response"
          ? frame.method ?? "response"
          : frame.type;

      this.emitIpcFrame({
        direction: "in",
        frame,
        method,
        threadId
      });

      if (frame.type !== "broadcast" || frame.method !== "thread-stream-state-changed") {
        return;
      }

      const params = frame.params;
      if (!params || typeof params !== "object") {
        return;
      }

      const conversationId = (params as Record<string, string>)["conversationId"];
      if (!conversationId || !conversationId.trim()) {
        return;
      }

      if (frame.sourceClientId && frame.sourceClientId.trim()) {
        this.threadOwnerById.set(conversationId, frame.sourceClientId.trim());
      }

      const current = this.streamEventsByThreadId.get(conversationId) ?? [];
      current.push(frame);
      if (current.length > 400) {
        current.splice(0, current.length - 400);
      }
      this.streamEventsByThreadId.set(conversationId, current);
    });
  }

  private handleAppServerNotification(notification: JsonRpcNotification): void {
    const threadId = extractThreadIdFromNotificationParams(notification.params);

    // Emit as a synthetic IPC frame event for history/SSE.
    const syntheticFrame = notificationToSyntheticFrame(notification);
    this.emitIpcFrame({
      direction: "in",
      frame: syntheticFrame,
      method: notification.method,
      threadId
    });

    // Store per-thread for readStreamEvents.
    if (threadId) {
      const current = this.appServerNotificationsByThreadId.get(threadId) ?? [];
      current.push(notification);
      if (current.length > APP_SERVER_NOTIFICATION_LIMIT) {
        current.splice(0, current.length - APP_SERVER_NOTIFICATION_LIMIT);
      }
      this.appServerNotificationsByThreadId.set(threadId, current);
    }

    // Trigger state change to notify SSE clients.
    this.notifyStateChanged();
  }

  private handleAppServerRequest(request: JsonRpcServerRequest): void {
    const threadId = extractThreadIdFromNotificationParams(request.params);

    logger.info(
      {
        method: request.method,
        requestId: request.id,
        threadId
      },
      "codex-app-server-request"
    );

    // Emit as a synthetic IPC frame event for history.
    const syntheticFrame: IpcFrame = {
      type: "request",
      requestId: String(request.id),
      method: request.method,
      params: request.params,
      sourceClientId: "app-server"
    };
    this.emitIpcFrame({
      direction: "in",
      frame: syntheticFrame,
      method: request.method,
      threadId
    });

    // Store the pending request for frontend approval flow.
    this.pendingServerRequests.set(request.id, request);

    // Trigger state change to notify SSE clients of pending approval.
    this.notifyStateChanged();
  }

  public onIpcFrame(listener: (event: CodexIpcFrameEvent) => void): () => void {
    this.ipcFrameListeners.add(listener);
    return () => {
      this.ipcFrameListeners.delete(listener);
    };
  }

  public getRuntimeState(): CodexAgentRuntimeState {
    return { ...this.runtimeState };
  }

  public getThreadOwnerCount(): number {
    return this.threadOwnerById.size;
  }

  public getPendingServerRequests(): JsonRpcServerRequest[] {
    return Array.from(this.pendingServerRequests.values());
  }

  public respondToServerRequest(
    requestId: number,
    result?: unknown,
    error?: { code: number; message: string; data?: unknown }
  ): void {
    const pending = this.pendingServerRequests.get(requestId);
    if (!pending) {
      logger.warn({ requestId }, "codex-app-server-request-not-found");
      return;
    }

    this.pendingServerRequests.delete(requestId);
    this.appClient.respondToServerRequest(requestId, result, error);

    logger.info(
      {
        method: pending.method,
        requestId,
        responded: error ? "error" : "success"
      },
      "codex-app-server-request-responded"
    );
  }

  public isThreadNotLoadedError(error: Error): boolean {
    if (!(error instanceof AppServerRpcError)) {
      return false;
    }

    if (error.code !== -32600) {
      return false;
    }

    return error.message.includes("thread not loaded");
  }

  public isConversationNotFoundError(error: unknown): boolean {
    if (!(error instanceof AppServerRpcError)) {
      return false;
    }

    if (error.code !== -32600) {
      return false;
    }

    return error.message.includes("conversation not found");
  }

  public isEnabled(): boolean {
    return true;
  }

  public isConnected(): boolean {
    return this.runtimeState.codexAvailable && this.runtimeState.appReady;
  }

  public isIpcReady(): boolean {
    return this.runtimeState.ipcConnected && this.runtimeState.ipcInitialized;
  }

  public async start(): Promise<void> {
    this.started = true;
    await this.bootstrapConnections();
  }

  public async stop(): Promise<void> {
    this.started = false;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    await this.ipcClient.disconnect();
    await this.appClient.close();
  }

  public async listThreads(input: AgentListThreadsInput): Promise<AgentListThreadsResult> {
    this.ensureCodexAvailable();

    const result = await this.runAppServerCall(() =>
      input.all
        ? this.appClient.listThreadsAll(
            input.cursor
              ? {
                  limit: input.limit,
                  archived: input.archived,
                  cursor: input.cursor,
                  maxPages: input.maxPages
                }
              : {
                  limit: input.limit,
                  archived: input.archived,
                  maxPages: input.maxPages
                }
          )
        : this.appClient.listThreads(
            input.cursor
              ? {
                  limit: input.limit,
                  archived: input.archived,
                  cursor: input.cursor
                }
              : {
                  limit: input.limit,
                  archived: input.archived
                }
          )
    );

    return {
      data: result.data,
      nextCursor: result.nextCursor ?? null,
      ...(typeof result.pages === "number" ? { pages: result.pages } : {}),
      ...(typeof result.truncated === "boolean" ? { truncated: result.truncated } : {})
    };
  }

  public async createThread(input: AgentCreateThreadInput): Promise<AgentCreateThreadResult> {
    this.ensureCodexAvailable();

    const cwd = input.cwd;
    if (!cwd || cwd.trim().length === 0) {
      throw new Error("Codex thread creation requires cwd");
    }

    const result = await this.runAppServerCall(() =>
      this.appClient.startThread({
        cwd,
        ...(input.model ? { model: input.model } : {}),
        ...(input.modelProvider ? { modelProvider: input.modelProvider } : {}),
        ...(input.personality ? { personality: input.personality } : {}),
        ...(input.sandbox ? { sandbox: input.sandbox } : {}),
        ...(input.approvalPolicy ? { approvalPolicy: input.approvalPolicy } : {}),
        ...(typeof input.ephemeral === "boolean" ? { ephemeral: input.ephemeral } : {})
      })
    );

    return {
      threadId: result.thread.id,
      thread: result.thread,
      model: result.model,
      modelProvider: result.modelProvider,
      cwd: result.cwd,
      approvalPolicy: result.approvalPolicy,
      sandbox: result.sandbox,
      reasoningEffort: result.reasoningEffort
    };
  }

  public async readThread(input: AgentReadThreadInput): Promise<AgentReadThreadResult> {
    this.ensureCodexAvailable();
    const result = await this.runAppServerCall(() =>
      this.appClient.readThread(input.threadId, input.includeTurns)
    );
    return {
      thread: result.thread
    };
  }

  public async sendMessage(input: AgentSendMessageInput): Promise<void> {
    this.ensureCodexAvailable();
    if (input.isSteering === true) {
      throw new Error("Steering messages are not supported on this endpoint.");
    }

    // Path 1: IPC is available — use the desktop app's thread-follower protocol.
    if (this.isIpcReady()) {
      const mappedOwnerClientId = this.threadOwnerById.get(input.threadId);
      const overrideOwnerClientId = input.ownerClientId;
      const ownerClientId = mappedOwnerClientId && mappedOwnerClientId.trim()
        ? mappedOwnerClientId.trim()
        : overrideOwnerClientId && overrideOwnerClientId.trim()
          ? overrideOwnerClientId.trim()
          : null;

      if (ownerClientId) {
        const readResult = await this.runAppServerCall(() =>
          this.appClient.readThread(input.threadId, true)
        );

        let turnStartTemplate: ReturnType<typeof findLatestTurnParamsTemplate> | null = null;
        try {
          turnStartTemplate = findLatestTurnParamsTemplate(readResult.thread);
        } catch {
          turnStartTemplate = null;
        }

        await this.service.sendMessage({
          threadId: input.threadId,
          ownerClientId,
          text: input.text,
          ...(input.cwd ? { cwd: input.cwd } : {}),
          ...(typeof input.isSteering === "boolean" ? { isSteering: input.isSteering } : {}),
          turnStartTemplate
        });
        return;
      }
    }

    // Path 2: Use app-server directly (works without IPC/Desktop App).
    // Try turn/start first (modern API), fall back to sendUserMessage (v1 API).
    try {
      await this.runAppServerCall(() =>
        this.appClient.startTurn({
          threadId: input.threadId,
          input: [{ type: "text", text: input.text }],
          ...(input.cwd ? { cwd: input.cwd } : {})
        })
      );
      return;
    } catch (error) {
      // If turn/start fails (e.g. thread not loaded), try the legacy path.
      logger.debug(
        {
          threadId: input.threadId,
          error: toErrorMessage(error)
        },
        "codex-turn-start-fallback"
      );
    }

    try {
      await this.runAppServerCall(() =>
        this.appClient.sendUserMessage(input.threadId, input.text)
      );
      return;
    } catch (error) {
      if (!this.isConversationNotFoundError(error)) {
        throw error;
      }
    }

    await this.runAppServerCall(() =>
      this.appClient.resumeThread(input.threadId, { persistExtendedHistory: true })
    );
    await this.runAppServerCall(() =>
      this.appClient.sendUserMessage(input.threadId, input.text)
    );
  }

  public async interrupt(input: AgentInterruptInput): Promise<void> {
    this.ensureCodexAvailable();

    // Path 1: IPC is available — use the desktop app's thread-follower protocol.
    if (this.isIpcReady()) {
      const ownerClientId = resolveOwnerClientId(
        this.threadOwnerById,
        input.threadId,
        input.ownerClientId
      );

      await this.service.interrupt({
        threadId: input.threadId,
        ownerClientId
      });
      return;
    }

    // Path 2: Use app-server directly (works without IPC/Desktop App).
    await this.runAppServerCall(() =>
      this.appClient.interruptTurn(input.threadId)
    );
  }

  public async listModels(limit: number) {
    this.ensureCodexAvailable();
    return this.runAppServerCall(() => this.appClient.listModels(limit));
  }

  public async listCollaborationModes() {
    this.ensureCodexAvailable();
    return this.runAppServerCall(() => this.appClient.listCollaborationModes());
  }

  public async setCollaborationMode(input: AgentSetCollaborationModeInput): Promise<{ ownerClientId: string }> {
    this.ensureCodexAvailable();

    if (this.isIpcReady()) {
      const ownerClientId = resolveOwnerClientId(
        this.threadOwnerById,
        input.threadId,
        input.ownerClientId
      );

      await this.service.setCollaborationMode({
        threadId: input.threadId,
        ownerClientId,
        collaborationMode: input.collaborationMode
      });

      return {
        ownerClientId
      };
    }

    // Without IPC, collaboration mode setting is not supported.
    throw new Error("Collaboration mode changes require the Codex Desktop App");
  }

  public async submitUserInput(
    input: AgentSubmitUserInputInput
  ): Promise<{ ownerClientId: string; requestId: number }> {
    this.ensureCodexAvailable();

    // Check if this request is a pending app-server request (no IPC needed).
    const pendingServerRequest = this.pendingServerRequests.get(input.requestId);
    if (pendingServerRequest) {
      const response = parseUserInputResponsePayload(input.response);
      this.respondToServerRequest(input.requestId, response);
      return {
        ownerClientId: "app-server",
        requestId: input.requestId
      };
    }

    // Fall back to IPC path.
    if (!this.isIpcReady()) {
      throw new Error("No pending request found and Desktop IPC is not connected");
    }

    const ownerClientId = resolveOwnerClientId(
      this.threadOwnerById,
      input.threadId,
      input.ownerClientId
    );

    await this.service.submitUserInput({
      threadId: input.threadId,
      ownerClientId,
      requestId: input.requestId,
      response: parseUserInputResponsePayload(input.response)
    });

    return {
      ownerClientId,
      requestId: input.requestId
    };
  }

  public async readLiveState(threadId: string): Promise<AgentThreadLiveState> {
    // Path 1: IPC stream events available — use the existing reduction logic.
    const rawEvents = this.streamEventsByThreadId.get(threadId) ?? [];
    if (rawEvents.length > 0) {
      return this.reduceLiveStateFromIpc(threadId, rawEvents);
    }

    // Path 2: No IPC but app-server notifications exist — read thread from app-server.
    const hasNotifications = (this.appServerNotificationsByThreadId.get(threadId) ?? []).length > 0;
    if (hasNotifications && this.runtimeState.appReady) {
      try {
        const result = await this.runAppServerCall(() =>
          this.appClient.readThread(threadId, true)
        );
        return {
          ownerClientId: this.threadOwnerById.get(threadId) ?? "app-server",
          conversationState: result.thread,
          liveStateError: null
        };
      } catch (error) {
        logger.debug(
          {
            threadId,
            error: toErrorMessage(error)
          },
          "codex-live-state-app-server-fallback-failed"
        );
      }
    }

    return {
      ownerClientId: this.threadOwnerById.get(threadId) ?? null,
      conversationState: null,
      liveStateError: null
    };
  }

  private reduceLiveStateFromIpc(threadId: string, rawEvents: IpcFrame[]): AgentThreadLiveState {
    const events: ReturnType<typeof parseThreadStreamStateChangedBroadcast>[] = [];
    const validRawEvents: IpcFrame[] = [];
    let invalidEventCount = 0;
    let firstInvalidEventError: string | null = null;

    for (const event of rawEvents) {
      try {
        events.push(parseThreadStreamStateChangedBroadcast(event));
        validRawEvents.push(event);
      } catch (error) {
        invalidEventCount += 1;
        if (!firstInvalidEventError) {
          firstInvalidEventError = toErrorMessage(error);
          logger.warn(
            {
              threadId,
              error: firstInvalidEventError,
              ...(error instanceof ProtocolValidationError ? { issues: error.issues } : {}),
              rawPayload: event
            },
            "codex-invalid-thread-stream-event-detail"
          );
          writeInvalidStreamEventDetail({
            threadId,
            error: firstInvalidEventError,
            ...(error instanceof ProtocolValidationError ? { issues: error.issues } : {}),
            rawPayload: event,
            loggedAt: new Date().toISOString()
          });
        }
      }
    }

    if (invalidEventCount > 0) {
      logger.warn(
        {
          threadId,
          invalidEventCount,
          eventCount: rawEvents.length,
          error: firstInvalidEventError
        },
        "codex-invalid-thread-stream-events-pruned"
      );
      this.streamEventsByThreadId.set(threadId, validRawEvents);
    }

    if (events.length === 0) {
      return {
        ownerClientId: this.threadOwnerById.get(threadId) ?? null,
        conversationState: null,
        liveStateError: null
      };
    }

    try {
      const reduced = reduceThreadStreamEvents(events);
      const state = reduced.get(threadId);
      return {
        ownerClientId: state?.ownerClientId ?? this.threadOwnerById.get(threadId) ?? null,
        conversationState: state?.conversationState ?? null,
        liveStateError: null
      };
    } catch (error) {
      const details =
        error instanceof ThreadStreamReductionError
          ? {
              threadId: error.details.threadId,
              eventIndex: error.details.eventIndex,
              patchIndex: error.details.patchIndex
            }
          : null;
      logger.error(
        {
          threadId,
          eventCount: events.length,
          error: toErrorMessage(error),
          details
        },
        "codex-thread-stream-reduction-failed"
      );
      return {
        ownerClientId: this.threadOwnerById.get(threadId) ?? null,
        conversationState: null,
        liveStateError: {
          kind: "reductionFailed",
          message: toErrorMessage(error),
          eventIndex: details?.eventIndex ?? null,
          patchIndex: details?.patchIndex ?? null
        }
      };
    }
  }

  public async readStreamEvents(threadId: string, limit: number): Promise<AgentThreadStreamEvents> {
    // Merge IPC stream events and app-server notifications.
    const ipcEvents = this.streamEventsByThreadId.get(threadId) ?? [];
    const appNotifications = this.appServerNotificationsByThreadId.get(threadId) ?? [];
    const syntheticFrames = appNotifications.map(notificationToSyntheticFrame);

    // Combine and return the most recent events.
    const combined = [...ipcEvents, ...syntheticFrames];

    return {
      ownerClientId: this.threadOwnerById.get(threadId) ?? null,
      events: combined.slice(-limit)
    };
  }

  public async replayRequest(
    method: string,
    params: IpcRequestFrame["params"],
    options: SendRequestOptions = {}
  ): Promise<IpcResponseFrame["result"]> {
    this.ensureIpcReady();
    const previewFrame: IpcFrame = {
      type: "request",
      requestId: "monitor-preview-request-id",
      method,
      params,
      targetClientId: options.targetClientId,
      version: options.version
    };
    this.emitIpcFrame({
      direction: "out",
      frame: previewFrame,
      method,
      threadId: extractThreadId(previewFrame)
    });

    const response = await this.ipcClient.sendRequestAndWait(method, params, options);
    return response.result;
  }

  public replayBroadcast(
    method: string,
    params: IpcRequestFrame["params"],
    options: SendRequestOptions = {}
  ): void {
    this.ensureIpcReady();
    const previewFrame: IpcFrame = {
      type: "broadcast",
      method,
      params,
      targetClientId: options.targetClientId,
      version: options.version
    };
    this.emitIpcFrame({
      direction: "out",
      frame: previewFrame,
      method,
      threadId: extractThreadId({
        type: "request",
        requestId: "monitor-preview-request-id",
        method,
        params,
        targetClientId: options.targetClientId,
        version: options.version
      })
    });

    this.ipcClient.sendBroadcast(method, params, options);
  }

  private emitIpcFrame(event: CodexIpcFrameEvent): void {
    for (const listener of this.ipcFrameListeners) {
      listener(event);
    }
  }

  private notifyStateChanged(): void {
    if (this.onStateChange) {
      this.onStateChange();
    }
  }

  private setRuntimeState(next: CodexAgentRuntimeState): void {
    const isSameState = this.runtimeState.appReady === next.appReady
      && this.runtimeState.ipcConnected === next.ipcConnected
      && this.runtimeState.ipcInitialized === next.ipcInitialized
      && this.runtimeState.codexAvailable === next.codexAvailable
      && this.runtimeState.lastError === next.lastError;

    if (isSameState) {
      return;
    }

    this.runtimeState = next;
    this.notifyStateChanged();
  }

  private patchRuntimeState(patch: Partial<CodexAgentRuntimeState>): void {
    this.setRuntimeState({
      ...this.runtimeState,
      ...patch
    });
  }

  private ensureCodexAvailable(): void {
    if (!this.runtimeState.codexAvailable) {
      throw new Error("Codex backend is not available");
    }
  }

  private ensureIpcReady(): void {
    if (!this.isIpcReady()) {
      throw new Error(this.runtimeState.lastError ?? "Desktop IPC is not connected");
    }
  }

  private scheduleIpcReconnect(): void {
    if (this.reconnectTimer || !this.runtimeState.codexAvailable || !this.started) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.bootstrapConnections();
    }, this.reconnectDelayMs);
  }

  private async runAppServerCall<T>(operation: () => Promise<T>): Promise<T> {
    try {
      const result = await operation();
      this.patchRuntimeState({
        appReady: true,
        lastError: null
      });
      return result;
    } catch (error) {
      this.patchRuntimeState({
        appReady: !(error instanceof AppServerTransportError),
        lastError: toErrorMessage(error)
      });
      throw error;
    }
  }

  private async bootstrapConnections(): Promise<void> {
    if (this.bootstrapInFlight) {
      return this.bootstrapInFlight;
    }

    this.bootstrapInFlight = (async () => {
      try {
        await this.runAppServerCall(() =>
          this.appClient.listThreads({ limit: 1, archived: false })
        );
      } catch (error) {
        const message = toErrorMessage(error);
        const isSpawnError = message.includes("ENOENT") ||
          message.includes("not found") ||
          (error instanceof Error && "code" in error &&
            (error as NodeJS.ErrnoException).code === "ENOENT");

        if (isSpawnError) {
          this.patchRuntimeState({
            codexAvailable: false,
            lastError: message
          });
          logger.warn({ error: message }, "codex-not-found");
        }
      }

      if (!this.runtimeState.codexAvailable) {
        this.bootstrapInFlight = null;
        return;
      }

      try {
        if (!this.ipcClient.isConnected()) {
          await this.ipcClient.connect();
        }
        this.patchRuntimeState({
          ipcConnected: true
        });

        await this.ipcClient.initialize(this.label);
        this.patchRuntimeState({
          ipcInitialized: true
        });
      } catch (error) {
        // IPC connection is optional — the adapter works without it using
        // app-server notifications for real-time updates and app-server
        // methods for turn control.
        const errorMessage = toErrorMessage(error);
        logger.info(
          { error: errorMessage },
          "codex-ipc-unavailable-using-app-server-only"
        );
        this.patchRuntimeState({
          ipcInitialized: false,
          ipcConnected: this.ipcClient.isConnected(),
          lastError: null
        });
        this.scheduleIpcReconnect();
      } finally {
        this.bootstrapInFlight = null;
      }
    })();

    return this.bootstrapInFlight;
  }
}

function toErrorMessage(error: Error | string | unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return String(error);
}

function normalizeStderrLine(line: string): string {
  return line.replace(ANSI_ESCAPE_REGEX, "").trim();
}

function isKnownBenignAppServerStderr(line: string): boolean {
  return (
    line.includes("codex_core::rollout::list") &&
    line.includes("state db missing rollout path for thread")
  );
}

function writeInvalidStreamEventDetail(detail: Record<string, unknown>): void {
  try {
    fs.appendFileSync(
      INVALID_STREAM_EVENTS_LOG_PATH,
      JSON.stringify(detail) + "\n",
      { encoding: "utf8" }
    );
  } catch (error) {
    logger.warn(
      {
        path: INVALID_STREAM_EVENTS_LOG_PATH,
        error: toErrorMessage(error)
      },
      "codex-invalid-thread-stream-event-detail-write-failed"
    );
  }
}

function extractThreadId(frame: IpcFrame): string | null {
  if (frame.type === "broadcast" && frame.method === "thread-stream-state-changed") {
    const params = frame.params;
    if (!params || typeof params !== "object") {
      return null;
    }

    const conversationId = (params as Record<string, string>)["conversationId"];
    if (typeof conversationId === "string" && conversationId.trim()) {
      return conversationId.trim();
    }

    return null;
  }

  if (frame.type !== "request") {
    return null;
  }

  const params = frame.params;
  if (!params || typeof params !== "object") {
    return null;
  }

  const asRecord = params as Record<string, string>;
  const candidates = [
    asRecord["conversationId"],
    asRecord["threadId"],
    asRecord["turnId"]
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }

  return null;
}
