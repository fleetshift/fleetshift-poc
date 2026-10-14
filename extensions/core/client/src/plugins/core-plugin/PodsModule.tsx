import { Bullseye, Spinner } from "@patternfly/react-core";
import { lazy, Suspense } from "react";
import { Route, Routes } from "react-router-dom";

const ClusterPodsTab = lazy(() => import("./ClusterPodsTab"));
const PodDetailPage = lazy(() => import("./PodDetailPage"));

const Loading = () => (
  <Bullseye>
    <Spinner size="xl" />
  </Bullseye>
);

export default function PodsModule() {
  return (
    <Suspense fallback={<Loading />}>
      <Routes>
        <Route index element={<ClusterPodsTab clusterId={undefined} />} />
        <Route path=":clusterId/:podUid" element={<PodDetailPage />} />
      </Routes>
    </Suspense>
  );
}
