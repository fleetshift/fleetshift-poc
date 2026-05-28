export interface FleetShiftApi {
  fleetshift: {
    getPluginPagePath: (scope: string, module: string) => string | undefined;
  };
}
