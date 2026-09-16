import { useTabContext } from "./tabContext";
import { useTabsStore } from "../store/useTabsStore";
import { useEffect } from "react";
import { useLocation } from "react-router-dom";
import { usePageTitleStore } from "../store/usePageTitleStore";

export function usePublishTitle(title: string | null | undefined) {
  const tab = useTabContext();
  const { pathname } = useLocation();
  const setPageTitle = usePageTitleStore((s) => s.setPageTitle);

  useEffect(() => {
    if (title) {
      if (tab.active) setPageTitle(pathname, title);
      if (tab.id) useTabsStore.getState().setTitle(tab.id, title);
    }
  }, [pathname, title, setPageTitle, tab.active, tab.id]);
}
