import type { CodeRef } from "@openshift/dynamic-plugin-sdk";
import type { ComponentType } from "react";

export type SearchResultProps = {
  title: string;
  description: string;
};

export type SearchReservedProperties = {
  searchResult?: CodeRef<ComponentType<SearchResultProps>>;
  searchIcon?: CodeRef<ComponentType>;
};
