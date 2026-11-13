import { createHashRouter } from "react-router";
import Clipboard from "@/pages/Clipboard";
import ContextMenu, { ContextSubmenu } from "@/pages/ContextMenu";
import FileTransfer from "@/pages/FileTransfer";
import Onboarding from "@/pages/Onboarding";
import PairRequest from "@/pages/PairRequest";
import Preference from "@/pages/Preference";
import Preview from "@/pages/Preview";

export const router = createHashRouter([
  {
    Component: Clipboard,
    path: "/",
  },
  {
    Component: Preference,
    path: "/preference",
  },
  {
    Component: Onboarding,
    path: "/onboarding",
  },
  {
    Component: ContextMenu,
    path: "/context-menu",
  },
  {
    Component: ContextSubmenu,
    path: "/context-submenu",
  },
  {
    Component: Preview,
    path: "/preview",
  },
  {
    Component: PairRequest,
    path: "/pair-request",
  },
  {
    Component: FileTransfer,
    path: "/file-transfer",
  },
]);
