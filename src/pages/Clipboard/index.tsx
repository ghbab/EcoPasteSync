import SyncPairRequestListener from "@/components/SyncPairRequestListener";
import { useClipboardWindowEditableFocus } from "@/hooks/useClipboardWindowEditableFocus";
import Footer from "./components/Footer";
import Group from "./components/Group";
import Header from "./components/Header";
import List from "./components/List";

const Clipboard = () => {
  useClipboardWindowEditableFocus();

  return (
    <div
      className="flex size-screen flex-col overflow-hidden bg-ant-container"
      data-tauri-drag-region
    >
      <Header />

      <Group />

      <List />

      <Footer />

      {/* 常驻监听局域网同步配对请求，仅剪贴板主窗口挂载。 */}
      <SyncPairRequestListener />
    </div>
  );
};

export default Clipboard;
