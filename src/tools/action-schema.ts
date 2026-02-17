import { z } from "zod";

export const optionalProjectKey = z.string().optional();

export const paginationFields = {
  limit: z.number().int().min(1).optional(),
  offset: z.number().int().min(0).optional(),
  query: z.string().optional(),
} as const;

export function actionInput<TAction extends string, TShape extends z.ZodRawShape>(
  action: TAction,
  shape: TShape,
) {
  return z.object({
    action: z.literal(action),
    ...shape,
  });
}

export function actionSchema(options: [z.ZodTypeAny, ...z.ZodTypeAny[]]) {
  return z.discriminatedUnion("action", options as never);
}
