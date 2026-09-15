import { createClient } from "@hey-api/openapi-ts";
import meow from "meow";
import fs from "node:fs";
import path from "node:path";
import { parse } from "yaml";

const cli = meow(
  `
  Usage
    $ generate-api-client <input>

  Options
    --help     Show this help message
    --input    The input file for generating the API client
    --output   The output directory for the generated API client
`,
  {
    importMeta: import.meta,
    flags: {
      input: {
        type: "string",
        description: "The input file for generating the API client",
        isRequired: true,
      },
      output: {
        type: "string",
        description: "The output directory for the generated API client",
        isRequired: true,
      },
    },
  },
);

const { input, output } = cli.flags;

const spec = parse(fs.readFileSync(path.resolve(process.cwd(), input), "utf8"));
const uiSpec = parse(
  fs.readFileSync(path.resolve(process.cwd(), "openapi/ui.yaml"), "utf8"),
);
spec.paths = { ...spec.paths, ...uiSpec.paths };
spec.definitions = { ...spec.definitions, ...uiSpec.definitions };

function normalizeRefs(value) {
  if (Array.isArray(value)) {
    value.forEach(normalizeRefs);
    return;
  }
  if (!value || typeof value !== "object") return;
  if (value.$ref) {
    delete value.type;
    return;
  }
  Object.values(value).forEach(normalizeRefs);
}

normalizeRefs(spec);

await createClient({
  input: spec,
  output,
  plugins: ["@hey-api/client-fetch"],
});
