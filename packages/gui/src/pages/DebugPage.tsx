import { useState } from "react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionToggle,
  Card,
  CardBody,
  CodeBlock,
  CodeBlockCode,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import { useScalprum } from "@scalprum/react-core";
import { useAppConfig } from "../contexts/AppConfigContext";

const JsonBlock = ({ data }: { data: unknown }) => (
  <CodeBlock>
    <CodeBlockCode>{JSON.stringify(data, null, 2)}</CodeBlockCode>
  </CodeBlock>
);

export const DebugPage = () => {
  const { scalprumConfig, pluginPages, navLayout, pluginEntries, assetsHost } =
    useAppConfig();
  const { config: runtimeConfig } = useScalprum();

  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const toggle = (id: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const loadedPlugins = Object.keys(runtimeConfig);

  return (
    <Stack hasGutter>
      <StackItem>
        <Title headingLevel="h1">Debug — Plugin Discovery</Title>
      </StackItem>

      <StackItem>
        <Card>
          <CardBody className="pf-v6-u-p-0">
            <Accordion
              isBordered
              asDefinitionList={false}
              aria-label="Debug panels"
            >
              <AccordionItem isExpanded={expanded.has("plugin-registry")}>
                <AccordionToggle
                  id="plugin-registry"
                  onClick={() => toggle("plugin-registry")}
                >
                  Plugin Registry ({pluginEntries.length} entries)
                </AccordionToggle>
                <AccordionContent>
                  <JsonBlock
                    data={pluginEntries.map((e) => ({
                      name: e.name,
                      key: e.key,
                      label: e.label,
                      persona: e.persona,
                      hasManifest: !!e.pluginManifest,
                    }))}
                  />
                </AccordionContent>
              </AccordionItem>

              <AccordionItem isExpanded={expanded.has("scalprum-config")}>
                <AccordionToggle
                  id="scalprum-config"
                  onClick={() => toggle("scalprum-config")}
                >
                  Scalprum Config ({loadedPlugins.length} modules) — Assets:{" "}
                  <code>{assetsHost}</code>
                </AccordionToggle>
                <AccordionContent>
                  <JsonBlock
                    data={Object.fromEntries(
                      Object.entries(scalprumConfig).map(([k, v]) => [
                        k,
                        {
                          name: (v as Record<string, unknown>).name,
                          manifestLocation: (v as Record<string, unknown>)
                            .manifestLocation,
                          hasPluginManifest: !!(v as Record<string, unknown>)
                            .pluginManifest,
                        },
                      ]),
                    )}
                  />
                </AccordionContent>
              </AccordionItem>

              <AccordionItem isExpanded={expanded.has("plugin-pages")}>
                <AccordionToggle
                  id="plugin-pages"
                  onClick={() => toggle("plugin-pages")}
                >
                  Plugin Pages / Routes ({pluginPages.length})
                </AccordionToggle>
                <AccordionContent>
                  <JsonBlock data={pluginPages} />
                </AccordionContent>
              </AccordionItem>

              <AccordionItem isExpanded={expanded.has("nav-layout")}>
                <AccordionToggle
                  id="nav-layout"
                  onClick={() => toggle("nav-layout")}
                >
                  Nav Layout ({navLayout.length} entries)
                </AccordionToggle>
                <AccordionContent>
                  <JsonBlock data={navLayout} />
                </AccordionContent>
              </AccordionItem>
            </Accordion>
          </CardBody>
        </Card>
      </StackItem>
    </Stack>
  );
};
