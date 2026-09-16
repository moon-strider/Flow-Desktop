import { createContext, useContext, type RefObject } from "react";

export interface TabContextValue {
  id: string | null;
  active: boolean;
  ownsPip: boolean;
  rootRef: RefObject<HTMLDivElement | null> | null;
}

export const TabContext = createContext<TabContextValue>({ id: null, active: true, ownsPip: false, rootRef: null });
export const useTabContext = () => useContext(TabContext);
