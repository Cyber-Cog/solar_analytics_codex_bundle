"use client";

import { createContext, useContext, useState, useEffect, useCallback } from "react";
import type { PlantResponse } from "@/types";

interface PlantContextValue {
  plants: PlantResponse[];
  selectedPlant: string;
  setSelectedPlant: (id: string) => void;
  currentPlant: PlantResponse | undefined;
  isLoading: boolean;
}

const PlantContext = createContext<PlantContextValue>({
  plants: [],
  selectedPlant: "",
  setSelectedPlant: () => {},
  currentPlant: undefined,
  isLoading: false,
});

export function PlantProvider({
  children,
  plants,
  isLoading,
}: {
  children: React.ReactNode;
  plants: PlantResponse[];
  isLoading: boolean;
}) {
  const [selectedPlant, setSelectedPlantState] = useState<string>(() => {
    if (typeof window === "undefined") return "";
    return localStorage.getItem("solar_selected_plant") ?? "";
  });

  useEffect(() => {
    if (!selectedPlant && plants.length > 0) {
      setSelectedPlantState(plants[0].plant_id);
    }
  }, [plants, selectedPlant]);

  const setSelectedPlant = useCallback((id: string) => {
    setSelectedPlantState(id);
    if (typeof window !== "undefined") localStorage.setItem("solar_selected_plant", id);
  }, []);

  const currentPlant = plants.find((p) => p.plant_id === selectedPlant);

  return (
    <PlantContext.Provider value={{ plants, selectedPlant, setSelectedPlant, currentPlant, isLoading }}>
      {children}
    </PlantContext.Provider>
  );
}

export function usePlantContext() {
  return useContext(PlantContext);
}
