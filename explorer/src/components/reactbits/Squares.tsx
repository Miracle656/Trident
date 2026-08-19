import { useEffect, useRef } from 'react';

// Adapted from React Bits (reactbits.dev) "Squares" animated background.
// Plain canvas + requestAnimationFrame, no extra dependencies. When the
// visitor prefers reduced motion the grid is drawn once and never animated.
interface SquaresProps {
  speed?: number;
  squareSize?: number;
  className?: string;
}

export default function Squares({ speed = 0.25, squareSize = 44, className }: SquaresProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    let raf = 0;
    let offset = 0;
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    const resize = () => {
      const dpr = window.devicePixelRatio || 1;
      canvas.width = canvas.offsetWidth * dpr;
      canvas.height = canvas.offsetHeight * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    const draw = () => {
      const w = canvas.offsetWidth;
      const h = canvas.offsetHeight;
      ctx.clearRect(0, 0, w, h);
      ctx.lineWidth = 1;

      const start = -squareSize + (offset % squareSize);
      for (let x = start; x < w + squareSize; x += squareSize) {
        for (let y = start; y < h + squareSize; y += squareSize) {
          // Fade the grid toward the edges so it reads as texture, not chrome.
          const cx = Math.abs(x + squareSize / 2 - w / 2) / (w / 2);
          const cy = Math.abs(y + squareSize / 2 - h / 2) / (h / 2);
          const fade = Math.max(0, 1 - Math.max(cx, cy));
          if (fade <= 0.02) continue;
          ctx.strokeStyle = `rgba(255, 74, 31, ${0.1 * fade})`;
          ctx.strokeRect(x, y, squareSize, squareSize);
        }
      }
    };

    const tick = () => {
      offset += speed;
      draw();
      raf = window.requestAnimationFrame(tick);
    };

    resize();
    draw();
    if (!reduceMotion) raf = window.requestAnimationFrame(tick);

    const onResize = () => {
      resize();
      draw();
    };
    window.addEventListener('resize', onResize);
    return () => {
      window.cancelAnimationFrame(raf);
      window.removeEventListener('resize', onResize);
    };
  }, [speed, squareSize]);

  return <canvas ref={canvasRef} aria-hidden="true" className={className} />;
}
