import { useFieldApi } from "@data-driven-forms/react-form-renderer";
import { useCallback, useMemo } from "react";

import type { GcpHcpFormData, NodepoolEntry } from "../CreateGcpHcpWizard";
import NodePoolsStep from "../NodePoolsStep";

interface DdfNodePoolsStepProps {
  name: string;
  [key: string]: unknown;
}

export default function DdfNodePoolsStep(props: DdfNodePoolsStepProps) {
  const { input } = useFieldApi(props);

  const formData = useMemo<GcpHcpFormData>(
    () => ({
      clusterId: "",
      endpointAccess: "",
      releaseVersion: "",
      channelGroup: "",
      nodepools: (input.value as NodepoolEntry[]) || [],
    }),
    [input.value],
  );

  const onChange = useCallback(
    <K extends keyof GcpHcpFormData>(_field: K, value: GcpHcpFormData[K]) => {
      input.onChange(value);
    },
    [input],
  );

  return <NodePoolsStep formData={formData} onChange={onChange} />;
}
