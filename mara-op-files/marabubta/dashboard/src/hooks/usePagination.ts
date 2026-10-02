// Marabunta - Licensed under the MIT License.
import { useState, useMemo, useCallback } from 'react';

export interface PaginationState {
  page: number;
  perPage: number;
  totalPages: number;
  total: number;
  setPage: (page: number) => void;
  setPerPage: (perPage: number) => void;
  setTotal: (total: number) => void;
  nextPage: () => void;
  prevPage: () => void;
  canNext: boolean;
  canPrev: boolean;
}

export function usePagination(
  initialPerPage: number = 25,
): PaginationState {
  const [page, setPageRaw] = useState(1);
  const [perPage, setPerPageRaw] = useState(initialPerPage);
  const [total, setTotal] = useState(0);

  const totalPages = useMemo(
    () => Math.max(1, Math.ceil(total / perPage)),
    [total, perPage],
  );

  const setPage = useCallback(
    (p: number) => {
      setPageRaw(Math.max(1, Math.min(p, totalPages)));
    },
    [totalPages],
  );

  const setPerPage = useCallback((pp: number) => {
    setPerPageRaw(pp);
    setPageRaw(1);
  }, []);

  const nextPage = useCallback(() => {
    setPageRaw((prev) => Math.min(prev + 1, totalPages));
  }, [totalPages]);

  const prevPage = useCallback(() => {
    setPageRaw((prev) => Math.max(prev - 1, 1));
  }, []);

  const canNext = page < totalPages;
  const canPrev = page > 1;

  return {
    page,
    perPage,
    totalPages,
    total,
    setPage,
    setPerPage,
    setTotal,
    nextPage,
    prevPage,
    canNext,
    canPrev,
  };
}
