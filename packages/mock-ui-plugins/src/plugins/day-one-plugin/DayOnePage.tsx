import { type ReactNode, lazy, Suspense, useState } from "react";
import { Routes, Route, useNavigate } from "react-router-dom";
import { PluginLink } from "@fleetshift/common";
import CompletionModal from "./CompletionModal";
import {
  Breadcrumb,
  BreadcrumbItem,
  Title,
  Card,
  CardHeader,
  CardTitle,
  CardBody,
  Gallery,
  Label,
} from "@patternfly/react-core";

const InitialSetupForm = lazy(() => import("./InitialSetupForm"));
const WelcomePage = lazy(() => import("./WelcomePage"));
const CreateClusterWizard = lazy(() => import("./CreateClusterWizard"));

interface ComponentCard {
  title: string;
  slug: string;
  description: string;
  status: "planned" | "in-progress" | "done";
  element?: ReactNode;
}

const components: ComponentCard[] = [
  {
    title: "Initial Setup",
    slug: "setup",
    description:
      "Day-one configuration form: backing store, auth provider, signing keys, and claim mapping.",
    status: "in-progress",
    element: (
      <Suspense>
        <InitialSetupForm />
      </Suspense>
    ),
  },
  {
    title: "Welcome",
    slug: "welcome",
    description:
      "Post-setup welcome screen: workload selection and first cluster creation.",
    status: "in-progress",
    element: (
      <Suspense>
        <WelcomePage />
      </Suspense>
    ),
  },
  {
    title: "Create Cluster",
    slug: "create-cluster",
    description:
      "Wizard to provision a new kind cluster via the deployment API.",
    status: "in-progress",
    element: (
      <Suspense>
        <CreateClusterWizard />
      </Suspense>
    ),
  },
];

const statusColor = {
  planned: "blue" as const,
  "in-progress": "orange" as const,
  done: "green" as const,
};

function SubPage({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div>
      <Breadcrumb style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}>
        <BreadcrumbItem
          render={({ className, ariaCurrent }) => (
            <PluginLink
              scope="day-one-plugin"
              module="DayOnePage"
              className={className}
              aria-current={ariaCurrent}
            >
              Day One
            </PluginLink>
          )}
        />
        <BreadcrumbItem isActive>{title}</BreadcrumbItem>
      </Breadcrumb>
      {children}
    </div>
  );
}

function Placeholder({ title }: { title: string }) {
  return (
    <div>
      <Title headingLevel="h1">{title}</Title>
      <p>Component not yet implemented.</p>
    </div>
  );
}

function ComponentGallery() {
  const navigate = useNavigate();
  const [completionOpen, setCompletionOpen] = useState(false);

  return (
    <div>
      <Title
        headingLevel="h1"
        style={{ marginBottom: "var(--pf-t--global--spacer--lg)" }}
      >
        Day One
      </Title>
      <Gallery hasGutter minWidths={{ default: "300px" }}>
        {components.map((c) => (
          <Card
            key={c.slug}
            isFullHeight
            isClickable
            isSelectable
            onClick={() => navigate(c.slug)}
            style={{ cursor: "pointer" }}
          >
            <CardHeader
              actions={{
                actions: (
                  <Label color={statusColor[c.status]}>{c.status}</Label>
                ),
              }}
            >
              <CardTitle>{c.title}</CardTitle>
            </CardHeader>
            <CardBody>{c.description}</CardBody>
          </Card>
        ))}
        <Card
          isFullHeight
          isClickable
          isSelectable
          onClick={() => setCompletionOpen(true)}
          style={{ cursor: "pointer" }}
        >
          <CardHeader
            actions={{
              actions: <Label color="orange">in-progress</Label>,
            }}
          >
            <CardTitle>Completion</CardTitle>
          </CardHeader>
          <CardBody>
            Success modal shown after the first cluster is created.
          </CardBody>
        </Card>
      </Gallery>
      <CompletionModal
        isOpen={completionOpen}
        onClose={() => setCompletionOpen(false)}
      />
    </div>
  );
}

export default function DayOnePage() {
  return (
    <Routes>
      <Route index element={<ComponentGallery />} />
      {components.map((c) => (
        <Route
          key={c.slug}
          path={c.slug}
          element={
            <SubPage title={c.title}>
              {c.element ?? <Placeholder title={c.title} />}
            </SubPage>
          }
        />
      ))}
    </Routes>
  );
}
