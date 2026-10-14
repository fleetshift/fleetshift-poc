import { render } from "ink";
import type { ReactNode } from "react";

import type { OutputFormat } from "./argv";
import { OutputProvider } from "./ui";

export async function print(
  element: ReactNode,
  output: OutputFormat = "table",
): Promise<void> {
  await new Promise<void>((resolve) => {
    const instance = render(
      <OutputProvider format={output}>{element}</OutputProvider>,
    );
    instance.waitUntilExit().then(() => {
      instance.unmount();
      resolve();
    });
  });
}
