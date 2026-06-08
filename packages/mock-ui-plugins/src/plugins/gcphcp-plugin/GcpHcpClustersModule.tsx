import { lazy, Suspense } from "react";
import { Routes, Route } from "react-router-dom";
import { Bullseye, Spinner } from "@patternfly/react-core";

const GcpHcpClustersPage = lazy(() => import("./GcpHcpClustersPage"));
const GcpHcpClusterDetailPage = lazy(() => import("./GcpHcpClusterDetailPage"));

export default function GcpHcpClustersModule() {
  return (
    <Suspense
      fallback={
        <Bullseye>
          <Spinner size="xl" />
        </Bullseye>
      }
    >
      <Routes>
        <Route index element={<GcpHcpClustersPage />} />
        <Route path=":clusterId" element={<GcpHcpClusterDetailPage />} />
      </Routes>
    </Suspense>
  );
}
