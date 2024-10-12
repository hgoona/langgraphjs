export const debugLog = (debugMode: boolean, ...logItems: unknown[]) => {
    debugMode ? console.log(...logItems) : undefined;
};