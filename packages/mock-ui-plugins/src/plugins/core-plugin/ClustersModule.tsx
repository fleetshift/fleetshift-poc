import { lazy, Suspense } from "react";
import { Routes, Route } from "react-router-dom";
import { Bullseye, Spinner } from "@patternfly/react-core";

const ClustersPage = lazy(() => import("./ClustersPage"));
const ClusterDetailPage = lazy(() => import("./ClusterDetailPage"));
const ClusterProviderSelectionPage = lazy(
  () => import("./cluster-providers/ClusterProviderSelectionPage"),
);
const ClusterProviderWizardPage = lazy(
  () => import("./cluster-providers/ClusterProviderWizardPage"),
);

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
        <Route path="create" element={<ClusterProviderSelectionPage />} />
        <Route
          path="create/:providerId"
          element={<ClusterProviderWizardPage />}
        />
        <Route path=":deploymentId" element={<ClusterDetailPage />} />
      </Routes>
    </Suspense>
  );
}
