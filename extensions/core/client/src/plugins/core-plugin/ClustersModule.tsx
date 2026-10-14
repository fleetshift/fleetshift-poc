import { Bullseye, Spinner } from "@patternfly/react-core";
import { lazy, Suspense } from "react";
import { Route, Routes } from "react-router-dom";

const ClustersPage = lazy(() => import("./ClustersPage"));
const ClusterDetailPage = lazy(() => import("./ClusterDetailPage"));

const Loading = () => (
  <Bullseye>
    <Spinner size="xl" />
  </Bullseye>
);

export default function ClustersModule() {
  return (
    <Suspense fallback={<Loading />}>
      <Routes>
        <Route index element={<ClustersPage />} />
        <Route path=":clusterId" element={<ClusterDetailPage />} />
      </Routes>
    </Suspense>
  );
}
