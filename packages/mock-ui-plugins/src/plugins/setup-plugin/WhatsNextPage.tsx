import "./WhatsNextPage.scss";

import type {
  OnboardingActionCardProps,
  OnboardingActionFormProps,
} from "@fleetshift/common";
import type { CodeRef, Extension } from "@openshift/dynamic-plugin-sdk";
import { useResolvedExtensions } from "@openshift/dynamic-plugin-sdk";
import {
  Button,
  Content,
  Icon,
  Spinner,
  Split,
  SplitItem,
  Title,
} from "@patternfly/react-core";
import { CheckCircleIcon } from "@patternfly/react-icons";
import { type ComponentType, useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { useSetupProgress } from "./useSetupProgress";

type OnboardingActionExtension = Extension<
  "fleetshift.onboarding-action",
  {
    id: string;
    label: string;
    description?: string;
    icon: CodeRef<ComponentType>;
    card: CodeRef<ComponentType<OnboardingActionCardProps>>;
    form: CodeRef<ComponentType<OnboardingActionFormProps>>;
    overviewCta?: string;
    category?: string;
  }
>;

function isOnboardingAction(e: Extension): e is OnboardingActionExtension {
  return e.type === "fleetshift.onboarding-action";
}

interface WhatsNextPageProps {
  onSetupNext?: () => void;
  onSetupSkip?: () => void;
}

const WhatsNextPage = ({ onSetupNext, onSetupSkip }: WhatsNextPageProps) => {
  const [extensions, extensionsLoaded] =
    useResolvedExtensions(isOnboardingAction);
  const {
    progress,
    loaded: progressLoaded,
    setStepComplete,
  } = useSetupProgress();
  const [searchParams, setSearchParams] = useSearchParams();

  const isSetup = !!(onSetupNext || onSetupSkip);
  const actionId = searchParams.get("action");
  const completed = searchParams.get("completed");
  const categoryFilter = searchParams.get("category");

  const filteredExtensions = useMemo(
    () =>
      categoryFilter
        ? extensions.filter((e) => e.properties.category === categoryFilter)
        : extensions,
    [extensions, categoryFilter],
  );

  const activeExt = useMemo(
    () =>
      actionId
        ? extensions.find((e) => e.properties.id === actionId)
        : undefined,
    [actionId, extensions],
  );
  const FormComponent = activeExt?.properties.form;

  const handleComplete = useCallback(
    (id: string, label: string) => {
      setStepComplete(id, true);
      setSearchParams({ action: id, completed: label }, { replace: true });
    },
    [setStepComplete, setSearchParams],
  );

  const openForm = useCallback(
    (id: string) => {
      setSearchParams({ action: id });
    },
    [setSearchParams],
  );

  const backToCatalog = useCallback(() => {
    setSearchParams({});
  }, [setSearchParams]);

  const toolbar = (onSetupNext || onSetupSkip) && (
    <div className="ome-setup-whats-next__toolbar">
      <div className="ome-setup-whats-next__toolbar-inner">
        {onSetupNext && (
          <Button variant="primary" onClick={onSetupNext}>
            Continue to console
          </Button>
        )}
        {onSetupSkip && (
          <Button variant="link" onClick={onSetupSkip}>
            Skip for now
          </Button>
        )}
      </div>
    </div>
  );

  if (!extensionsLoaded || !progressLoaded) {
    return (
      <div className="ome-setup-whats-next">
        <div className="ome-setup-whats-next__body">
          <Spinner size="xl" aria-label="Loading onboarding actions" />
        </div>
        {toolbar}
      </div>
    );
  }

  if (actionId && completed) {
    return (
      <div className="ome-setup-whats-next">
        <div className="ome-setup-whats-next--centered">
          <Icon size="xl" status="success" className="pf-v6-u-mb-md">
            <CheckCircleIcon />
          </Icon>
          <Title headingLevel="h1" className="ome-setup-whats-next__title">
            {completed} configured
          </Title>
          <Content component="p" className="pf-v6-u-mb-lg">
            You can configure more addons or continue to the console.
          </Content>

          <Split hasGutter>
            <SplitItem>
              <Button variant="primary" onClick={backToCatalog}>
                Back to catalog
              </Button>
            </SplitItem>

            {onSetupNext && (
              <SplitItem>
                <Button variant="secondary" onClick={onSetupNext}>
                  Continue to console
                </Button>
              </SplitItem>
            )}
          </Split>
        </div>
      </div>
    );
  }

  if (actionId && activeExt && FormComponent && !completed) {
    return (
      <div className="ome-setup-whats-next">
        <FormComponent
          onComplete={() =>
            handleComplete(activeExt.properties.id, activeExt.properties.label)
          }
          onCancel={backToCatalog}
        />
      </div>
    );
  }

  const catalogTitle = isSetup
    ? "What do you want to do next?"
    : categoryFilter === "fleetshift.cluster-provider"
      ? "Cluster Providers"
      : "Extensions";

  return (
    <div className="ome-setup-whats-next">
      <div className="ome-setup-whats-next__body">
        <div className="ome-setup-whats-next__inner">
          <Title headingLevel="h1" className="ome-setup-whats-next__title">
            {catalogTitle}
          </Title>
          <Content component="p" className="pf-v6-u-mb-lg">
            Configure addons and integrations for your fleet.
          </Content>

          <div className="ome-setup-whats-next__catalog">
            {filteredExtensions.map((ext) => {
              const CardComponent = ext.properties.card;
              return (
                <CardComponent
                  key={ext.properties.id}
                  completed={!!progress[ext.properties.id]}
                  onConfigure={() => openForm(ext.properties.id)}
                />
              );
            })}
          </div>
        </div>
      </div>
      {toolbar}
    </div>
  );
};

export default WhatsNextPage;
