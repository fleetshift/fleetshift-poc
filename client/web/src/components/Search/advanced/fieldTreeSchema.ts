import { z } from "zod";

const fieldNodeSchema: z.ZodType<unknown> = z.lazy(() =>
  z
    .object({
      segment: z.string().min(1),
      path: z.string(),
      label: z.string().min(1),
      description: z.string().optional(),
      type: z.enum(["string", "number", "boolean"]).optional(),
      enumValues: z.array(z.string()).optional(),
      children: z.array(fieldNodeSchema).optional(),
      container: z.boolean().optional(),
    })
    .refine((node) => !!node.type || !!node.children, {
      message: "Node must have either type (leaf) or children (branch)",
    }),
);

export const fieldTreeSchema = z.array(fieldNodeSchema);

export function validateFieldTree(tree: unknown) {
  return fieldTreeSchema.parse(tree);
}
