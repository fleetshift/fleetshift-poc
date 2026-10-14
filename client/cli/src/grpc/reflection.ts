import { readFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as protobuf from "protobufjs";
import * as descriptor from "protobufjs/ext/descriptor";

import { flagString, hasFlag, type ParsedArgs } from "../argv";
import { loadAuthConfig, loadStoredTokens } from "../config";

const REFLECTION_PROTO = `syntax = "proto3";
package grpc.reflection.v1;
message ServerReflectionRequest {
  string host = 1;
  oneof message_request {
    string file_containing_symbol = 4;
    string list_services = 7;
  }
}
message ServerReflectionResponse {
  string valid_host = 1;
  ServerReflectionRequest original_request = 2;
  oneof message_response {
    FileDescriptorResponse file_descriptor_response = 4;
    ListServiceResponse list_services_response = 6;
    ErrorResponse error_response = 7;
  }
}
message ListServiceResponse { repeated ServiceResponse service = 1; }
message ServiceResponse { string name = 1; }
message FileDescriptorResponse { repeated bytes file_descriptor_proto = _file_desc_1; }
message ErrorResponse { int32 error_code = 1; string error_message = 2; }
service ServerReflection { rpc ServerReflectionInfo(stream ServerReflectionRequest) returns (stream ServerReflectionResponse); }
`.replace("_file_desc_1", "1");

export interface ReflectedResourceType {
  packageName: string;
  serviceName: string;
  singular: string;
  collection: string;
  qualifiedName: string;
}

interface ReflectionPackage {
  grpc: {
    reflection: {
      v1: { ServerReflection: grpc.ServiceClientConstructor };
    };
  };
}

async function reflectionClient(args?: ParsedArgs): Promise<{
  client: grpc.Client;
  loopback: boolean;
  secure: boolean;
}> {
  const directory = await mkdtemp(
    path.join(os.tmpdir(), "fleetctl-reflection-"),
  );
  const protoPath = path.join(directory, "reflection.proto");
  await writeFile(protoPath, REFLECTION_PROTO);
  const definition = protoLoader.loadSync(protoPath, {
    keepCase: false,
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(
    definition,
  ) as unknown as ReflectionPackage;
  const target =
    process.env.FLEETCTL_GRPC_SERVER ||
    process.env.FLEETSHIFT_GRPC_TARGET ||
    "localhost:50051";
  const loopback = isLoopbackTarget(target);
  const secure =
    (args ? hasFlag(args, "server-tls") : false) ||
    (!(args ? hasFlag(args, "server-insecure") : false) && !loopback);
  const authConfig = await loadAuthConfig(
    args ? flagString(args, "config-dir") || undefined : undefined,
  ).catch(() => undefined);
  const caFile =
    (args ? flagString(args, "server-ca-file") : "") ||
    authConfig?.oidc_ca_file ||
    process.env.FLEETSHIFT_CA_FILE;
  const credentials = secure
    ? grpc.credentials.createSsl(caFile ? readFileSync(caFile) : undefined)
    : grpc.credentials.createInsecure();
  const client = new loaded.grpc.reflection.v1.ServerReflection(
    target,
    credentials,
  );
  await rm(directory, { recursive: true, force: true });
  return { client, loopback, secure };
}

async function responseFor(
  request: Record<string, unknown>,
  args?: ParsedArgs,
): Promise<unknown> {
  const connection = await reflectionClient(args);
  const { client } = connection;
  const tokens = await loadStoredTokens(
    args ? flagString(args, "config-dir") || undefined : undefined,
  ).catch(() => undefined);
  const metadata = new grpc.Metadata();
  if (tokens?.access_token && (connection.secure || connection.loopback)) {
    metadata.set("authorization", `Bearer ${tokens.access_token}`);
  }
  try {
    return await new Promise((resolve, reject) => {
      let settled = false;
      const stream = (
        client as grpc.Client & {
          serverReflectionInfo(
            metadata: grpc.Metadata,
          ): grpc.ClientDuplexStream<unknown, unknown>;
        }
      ).serverReflectionInfo(metadata);
      stream.once("error", (error) => {
        settled = true;
        reject(error);
      });
      stream.once("data", (value) => {
        settled = true;
        resolve(value);
      });
      stream.once("end", () => {
        if (!settled)
          reject(new Error("reflection stream closed without response"));
      });
      stream.write(request);
      stream.end();
    });
  } finally {
    client.close();
  }
}

function isLoopbackTarget(target: string): boolean {
  const host = target.startsWith("[")
    ? target.slice(1, target.indexOf("]"))
    : target.split(":")[0];
  return host === "localhost" || host === "127.0.0.1" || host === "::1";
}

export async function listResourceTypes(
  args?: ParsedArgs,
): Promise<ReflectedResourceType[]> {
  const response = (await responseFor(
    {
      listServices: "",
      messageRequest: "listServices",
    },
    args,
  )) as {
    listServicesResponse?: { service?: { name?: string }[] };
  };
  return (response.listServicesResponse?.service ?? [])
    .map((service) => service.name ?? "")
    .filter((name) => name.endsWith("Service"))
    .filter((name) => !name.startsWith("fleetshift.v1."))
    .map(resourceTypeFromService)
    .filter((type): type is ReflectedResourceType => type !== undefined);
}

function resourceTypeFromService(
  serviceName: string,
): ReflectedResourceType | undefined {
  const separator = serviceName.lastIndexOf(".");
  if (separator < 1) return undefined;
  const packageName = serviceName.slice(0, separator);
  const localName = serviceName.slice(separator + 1, -"Service".length);
  if (!localName) return undefined;
  const collection = `${localName[0].toLowerCase()}${localName.slice(1)}s`;
  return {
    packageName,
    serviceName,
    singular: localName,
    collection,
    qualifiedName: `${packageName}/${collection}`,
  };
}

export async function describeResourceType(
  type: ReflectedResourceType,
  args?: ParsedArgs,
): Promise<{
  specType: string;
  methods: string[];
  fields: string[];
}> {
  const response = (await responseFor(
    {
      fileContainingSymbol: type.serviceName,
      messageRequest: "fileContainingSymbol",
    },
    args,
  )) as { fileDescriptorResponse?: { fileDescriptorProto?: Buffer[] } };
  const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
  if (files.length === 0) {
    throw new Error(
      `reflection descriptor response empty: ${JSON.stringify(response)}`,
    );
  }
  const descriptors = files.map((file) =>
    descriptor.FileDescriptorProto.decode(file),
  );
  const set = descriptor.FileDescriptorSet.create({ file: descriptors });
  const root = (
    protobuf.Root as unknown as {
      fromDescriptor(value: unknown): protobuf.Root;
    }
  ).fromDescriptor(set);
  root.resolveAll();
  const service = findNested<protobuf.Service>(
    root,
    type.serviceName,
    protobuf.Service,
  );
  const message = findNested<protobuf.Type>(
    root,
    `${type.packageName}.${type.singular}`,
    protobuf.Type,
  );
  if (!service || !message) {
    throw new Error(`descriptor missing ${type.serviceName}`);
  }
  const specField = message.fieldsArray.find((field) => field.name === "spec");
  const specMessage =
    specField?.resolvedType instanceof protobuf.Type
      ? specField.resolvedType
      : message;
  return {
    specType: (specMessage.fullName ?? message.fullName ?? "").replace(
      /^\./,
      "",
    ),
    methods: service.methodsArray.map(
      (method: { name: string }) => method.name,
    ),
    fields: flattenFields(specMessage),
  };
}

function flattenFields(
  message: protobuf.Type,
  seen = new Set<protobuf.Type>(),
): string[] {
  if (seen.has(message)) return [];
  seen.add(message);
  return message.fieldsArray.flatMap((field) => [
    `${field.type} ${field.name} = ${field.id}`,
    ...(field.resolvedType instanceof protobuf.Type
      ? flattenFields(field.resolvedType, seen)
      : []),
  ]);
}

function findNested<T extends protobuf.ReflectionObject>(
  namespace: protobuf.NamespaceBase,
  fullName: string,
  constructor: new (...args: never[]) => T,
): T | undefined {
  const wanted = fullName.replace(/^\./, "");
  const visit = (
    current: protobuf.NamespaceBase,
    prefix: string,
  ): T | undefined => {
    for (const child of current.nestedArray) {
      const name = prefix ? `${prefix}.${child.name}` : child.name;
      if (name === wanted && child instanceof constructor) return child;
      if (child instanceof protobuf.Namespace) {
        const found = visit(child, name);
        if (found) return found;
      }
    }
    return undefined;
  };
  return visit(namespace, "");
}
