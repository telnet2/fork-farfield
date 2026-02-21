import {
  type AppServerCollaborationModeListResponse,
  AppServerCollaborationModeListResponseSchema,
  type AppServerListModelsResponse,
  AppServerListModelsResponseSchema,
  type AppServerListThreadsResponse,
  AppServerListThreadsResponseSchema,
  type AppServerReadThreadResponse,
  AppServerReadThreadResponseSchema,
  AppServerSendUserMessageRequestSchema,
  AppServerSendUserMessageResponseSchema,
  type AppServerStartThreadResponse,
  AppServerStartThreadRequestSchema,
  AppServerStartThreadResponseSchema
} from "@farfield/protocol";
import { ProtocolValidationError } from "@farfield/protocol";
import { z } from "zod";
import {
  type AppServerTransport,
  type AppServerNotificationListener,
  type AppServerServerRequestListener,
  ChildProcessAppServerTransport,
  type ChildProcessAppServerTransportOptions
} from "./app-server-transport.js";
import type { JsonRpcServerRequest } from "./json-rpc.js";

function parseWithSchema<T>(
  schema: z.ZodType<T, z.ZodTypeDef, unknown>,
  value: unknown,
  context: string
): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    throw ProtocolValidationError.fromZod(context, parsed.error);
  }
  return parsed.data;
}

export interface ListThreadsOptions {
  limit: number;
  archived: boolean;
  cursor?: string;
}

export interface ListThreadsAllOptions {
  limit: number;
  archived: boolean;
  cursor?: string;
  maxPages: number;
}

export interface StartThreadOptions {
  cwd: string;
  model?: string;
  modelProvider?: string;
  personality?: string;
  sandbox?: string;
  approvalPolicy?: string;
  ephemeral?: boolean;
}

export interface StartTurnOptions {
  threadId: string;
  input: Array<{ type: "text"; text: string }>;
  cwd?: string;
  model?: string;
  effort?: string;
  approvalPolicy?: string;
}

const AppServerResumeThreadRequestSchema = z
  .object({
    threadId: z.string().min(1),
    persistExtendedHistory: z.boolean()
  })
  .passthrough();

export class AppServerClient {
  private readonly transport: AppServerTransport;

  public constructor(transportOrOptions: AppServerTransport | ChildProcessAppServerTransportOptions) {
    if ("request" in transportOrOptions && "close" in transportOrOptions) {
      this.transport = transportOrOptions;
      return;
    }

    this.transport = new ChildProcessAppServerTransport(transportOrOptions);
  }

  public onNotification(listener: AppServerNotificationListener): () => void {
    return this.transport.onNotification(listener);
  }

  public onServerRequest(listener: AppServerServerRequestListener): () => void {
    return this.transport.onServerRequest(listener);
  }

  public respondToServerRequest(
    id: number,
    result?: unknown,
    error?: { code: number; message: string; data?: unknown }
  ): void {
    this.transport.sendResponse(id, result, error);
  }

  public async close(): Promise<void> {
    await this.transport.close();
  }

  public async listThreads(options: ListThreadsOptions): Promise<AppServerListThreadsResponse> {
    const result = await this.transport.request("thread/list", {
      limit: options.limit,
      archived: options.archived,
      cursor: options.cursor ?? null
    });

    return parseWithSchema(AppServerListThreadsResponseSchema, result, "AppServerListThreadsResponse");
  }

  public async listThreadsAll(options: ListThreadsAllOptions): Promise<AppServerListThreadsResponse> {
    const listItems: AppServerListThreadsResponse["data"] = [];

    let cursor = options.cursor;
    let pages = 0;

    while (pages < options.maxPages) {
      const page = await this.listThreads(
        cursor
          ? {
              limit: options.limit,
              archived: options.archived,
              cursor
            }
          : {
              limit: options.limit,
              archived: options.archived
            }
      );

      listItems.push(...page.data);
      pages += 1;

      const nextCursor = page.nextCursor ?? null;
      if (!nextCursor || page.data.length === 0) {
        return {
          data: listItems,
          nextCursor: null,
          pages,
          truncated: false
        };
      }

      cursor = nextCursor;
    }

    return {
      data: listItems,
      nextCursor: cursor ?? null,
      pages,
      truncated: true
    };
  }

  public async readThread(threadId: string, includeTurns = true): Promise<AppServerReadThreadResponse> {
    const result = await this.transport.request("thread/read", {
      threadId,
      includeTurns
    });

    return parseWithSchema(AppServerReadThreadResponseSchema, result, "AppServerReadThreadResponse");
  }

  public async listModels(limit = 100): Promise<AppServerListModelsResponse> {
    const result = await this.transport.request("model/list", { limit });
    return parseWithSchema(AppServerListModelsResponseSchema, result, "AppServerListModelsResponse");
  }

  public async listCollaborationModes(): Promise<AppServerCollaborationModeListResponse> {
    const result = await this.transport.request("collaborationMode/list", {});
    return parseWithSchema(
      AppServerCollaborationModeListResponseSchema,
      result,
      "AppServerCollaborationModeListResponse"
    );
  }

  public async startThread(options: StartThreadOptions): Promise<AppServerStartThreadResponse> {
    const request = AppServerStartThreadRequestSchema.parse(options);
    const result = await this.transport.request("thread/start", request);
    return parseWithSchema(AppServerStartThreadResponseSchema, result, "AppServerStartThreadResponse");
  }

  public async startTurn(options: StartTurnOptions): Promise<void> {
    await this.transport.request("turn/start", {
      threadId: options.threadId,
      input: options.input,
      ...(options.cwd ? { cwd: options.cwd } : {}),
      ...(options.model ? { model: options.model } : {}),
      ...(options.effort ? { effort: options.effort } : {}),
      ...(options.approvalPolicy ? { approvalPolicy: options.approvalPolicy } : {})
    });
  }

  public async interruptTurn(threadId: string): Promise<void> {
    await this.transport.request("turn/interrupt", { threadId });
  }

  public async sendUserMessage(threadId: string, text: string): Promise<void> {
    const request = AppServerSendUserMessageRequestSchema.parse({
      conversationId: threadId,
      items: [
        {
          type: "text",
          data: {
            text
          }
        }
      ]
    });
    const result = await this.transport.request("sendUserMessage", request);
    parseWithSchema(AppServerSendUserMessageResponseSchema, result, "AppServerSendUserMessageResponse");
  }

  public async resumeThread(
    threadId: string,
    options?: { persistExtendedHistory?: boolean }
  ): Promise<AppServerReadThreadResponse> {
    const request = AppServerResumeThreadRequestSchema.parse({
      threadId,
      persistExtendedHistory: options?.persistExtendedHistory ?? true
    });
    const result = await this.transport.request("thread/resume", request);
    return parseWithSchema(AppServerReadThreadResponseSchema, result, "AppServerResumeThreadResponse");
  }
}
