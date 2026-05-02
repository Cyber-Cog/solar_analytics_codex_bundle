"use client";

import { useState, useCallback, useEffect } from "react";

const STORAGE_KEY = "solar_selected_plant";

export function usePlantStore(defaultPlant?: string) {
  const [selectedPlant, setSelectedPlantState] = useState<string>(() => {
    if (typeof window === "undefined") return defaultPlant ?? "";
    return localStorage.getItem(STORAGE_KEY) ?? defaultPlant ?? "";
  });

  const setSelectedPlant = useCallback((id: string) => {
    setSelectedPlantState(id);
    if (typeof window !== "undefined") localStorage.setItem(STORAGE_KEY, id);
  }, []);

  useEffect(() => {
    if (!selectedPlant && defaultPlant) setSelectedPlant(defaultPlant);
  }, [defaultPlant, selectedPlant, setSelectedPlant]);

  return { selectedPlant, setSelectedPlant };
}
