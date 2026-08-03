import { Bullseye, Spinner } from "@patternfly/react-core";
import { lazy, Suspense } from "react";
import { Route, Routes } from "react-router-dom";

const ClusterNodesTab = lazy(() => import("./ClusterNodesTab"));
const NodeDetailPage = lazy(() => import("./NodeDetailPage"));

const Loading = () => (
  <Bullseye>
    <Spinner size="xl" />
  </Bullseye>
);

export default function NodesModule() {
  return (
    <Suspense fallback={<Loading />}>
      <Routes>
        <Route index element={<ClusterNodesTab clusterId={undefined} />} />
        <Route path=":clusterId/:nodeUid" element={<NodeDetailPage />} />
      </Routes>
    </Suspense>
  );
}
