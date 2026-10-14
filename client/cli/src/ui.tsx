import { Box, Text } from "ink";
import React from "react";

import type { OutputFormat } from "./argv";

const OutputFormatContext = React.createContext<OutputFormat>("table");

export function OutputProvider({
  format,
  children,
}: {
  format: OutputFormat;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <OutputFormatContext.Provider value={format}>
      {children}
    </OutputFormatContext.Provider>
  );
}

export function useOutputFormat(): OutputFormat {
  return React.useContext(OutputFormatContext);
}

export function Output({
  value,
  error = false,
}: {
  value: string;
  error?: boolean;
}): React.ReactElement {
  return (
    <Box>
      <Text color={error ? "red" : undefined}>{value}</Text>
    </Box>
  );
}

export function JsonOutput({ value }: { value: unknown }): React.ReactElement {
  const serialized = JSON.stringify(value, null, 2) ?? "null";
  React.useLayoutEffect(() => {
    process.stdout.write(`${serialized}\n`);
  }, [serialized]);
  return React.createElement(React.Fragment);
}
