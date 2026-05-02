"use client";

import { useQuery } from "@tanstack/react-query";
import { Plants } from "@/lib/api";

export function usePlants() {
  return useQuery({
    queryKey: ["plants"],
    queryFn: () => Plants.list(),
    staleTime: 5 * 60_000,
  });
}
