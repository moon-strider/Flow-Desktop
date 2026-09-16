import { useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

export function TitleTooltip({ id, text, anchor }: { id: string; text: string; anchor: { x: number; y: number } }) {
  const ref = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: anchor.x + 12, top: anchor.y + 18 });

  useLayoutEffect(() => {
    const place = () => {
      const rect = ref.current?.getBoundingClientRect();
      if (!rect) return;
      const left = Math.max(12, Math.min(anchor.x + 12, window.innerWidth - rect.width - 12));
      const below = anchor.y + 18;
      const top = below + rect.height <= window.innerHeight - 12 ? below : Math.max(12, anchor.y - rect.height - 12);
      setPosition({ left, top });
    };
    place();
    window.addEventListener("resize", place);
    return () => window.removeEventListener("resize", place);
  }, [anchor]);

  return createPortal(
    <div ref={ref} id={id} role="tooltip" style={position}
      className="pointer-events-none fixed z-[500] max-w-[min(420px,calc(100vw-24px))] whitespace-normal break-words rounded-lg border border-outline-variant bg-surface-container-high px-3 py-2 text-sm leading-relaxed text-on-surface">
      {text}
    </div>,
    document.body,
  );
}
