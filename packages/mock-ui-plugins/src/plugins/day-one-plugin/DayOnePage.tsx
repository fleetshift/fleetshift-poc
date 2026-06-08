import { type ReactNode, useMemo } from "react";
import type { ComponentType } from "react";
import { Routes, Route, useNavigate } from "react-router-dom";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import { PluginLink } from "@fleetshift/common";
import {
  Breadcrumb,
  BreadcrumbItem,
  Bullseye,
  Card,
  CardHeader,
  CardTitle,
  CardBody,
  Content,
  Gallery,
  Label,
  Spinner,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";

interface SetupComponentProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

type SetupExtension = Extension<
  "fleetshift.setup",
  {
    id: string;
    label: string;
    description?: string;
    path: string;
    component: CodeRef<ComponentType<SetupComponentProps>>;
    requires: string[];
    requiresAuth: boolean;
    priority?: number;
  }
>;

function isSetupExtension(e: Extension): e is SetupExtension {
  return e.type === "fleetshift.setup";
}

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

function SetupGallery() {
  const [extensions, loaded] = useResolvedExtensions(isSetupExtension);
  const navigate = useNavigate();

  const sorted = useMemo(() => {
    const cpy = [...extensions];
    cpy.sort((a, b) => {
      const pa = a.properties.priority ?? 100;
      const pb = b.properties.priority ?? 100;
      if (pa !== pb) return pa - pb;
      return a.properties.label.localeCompare(b.properties.label);
    });
    return cpy;
  }, [extensions]);

  if (!loaded) {
    return (
      <Bullseye>
        <Spinner size="xl" />
      </Bullseye>
    );
  }

  return (
    <Stack hasGutter>
      <StackItem>
        <Title headingLevel="h1">Day One</Title>
        <Content component="p">
          Complete the setup steps below to get your management engine running.
        </Content>
      </StackItem>
      <StackItem>
        <Gallery hasGutter minWidths={{ default: "300px" }}>
          {sorted.map((ext) => {
            const { id, label, description, requires, requiresAuth, path } =
              ext.properties;
            return (
              <Card
                key={id}
                isFullHeight
                isClickable
                isSelectable
                onClick={() => navigate(path)}
                style={{ cursor: "pointer" }}
              >
                <CardHeader
                  actions={{
                    actions: (
                      <>
                        {requiresAuth && (
                          <Label color="blue" isCompact>
                            requires auth
                          </Label>
                        )}
                        {requires.length > 0 && (
                          <Label color="orange" isCompact>
                            {requires.length} dep
                            {requires.length > 1 ? "s" : ""}
                          </Label>
                        )}
                      </>
                    ),
                  }}
                >
                  <CardTitle>{label}</CardTitle>
                </CardHeader>
                <CardBody>{description ?? `Navigate to ${label}`}</CardBody>
              </Card>
            );
          })}
        </Gallery>
      </StackItem>
    </Stack>
  );
}

export default function DayOnePage() {
  const [extensions, loaded] = useResolvedExtensions(isSetupExtension);
  const navigate = useNavigate();

  const handleSetupNext = () => navigate("");

  const routesSorted = useMemo(() => {
    const cpy = [...extensions];
    cpy.sort((a, b) => b.properties.path.length - a.properties.path.length);
    return cpy;
  }, [extensions]);

  return (
    <Routes>
      <Route index element={<SetupGallery />} />
      {loaded &&
        routesSorted.map((ext) => {
          const Component = ext.properties.component;
          return (
            <Route
              key={ext.properties.id}
              path={`${ext.properties.path}/*`}
              element={
                <SubPage title={ext.properties.label}>
                  <Component onSetupNext={handleSetupNext} />
                </SubPage>
              }
            />
          );
        })}
    </Routes>
  );
}
