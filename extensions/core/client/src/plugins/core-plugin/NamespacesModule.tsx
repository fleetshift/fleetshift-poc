import { Bullseye, Spinner } from "@patternfly/react-core";
import { lazy, Suspense } from "react";
import { Route, Routes } from "react-router-dom";

const ClusterNamespacesTab = lazy(() => import("./ClusterNamespacesTab"));
const NamespaceDetailPage = lazy(() => import("./NamespaceDetailPage"));

const Loading = () => (
  <Bullseye>
    <Spinner size="xl" />
  </Bullseye>
);

export default function NamespacesModule() {
  return (
    <Suspense fallback={<Loading />}>
      <Routes>
        <Route index element={<ClusterNamespacesTab clusterId={undefined} />} />
        <Route path=":clusterId/:nsUid" element={<NamespaceDetailPage />} />
      </Routes>
    </Suspense>
  );
}
