import type { ReactNode } from 'react';
import { cn } from '../../lib/utils';

// Adapted from React Bits (reactbits.dev) "Gradient Text". CSS-only, so it
// server-renders with zero client JavaScript; the keyframes and the
// prefers-reduced-motion fallback live in src/styles/landing.css.
interface GradientTextProps {
  children: ReactNode;
  className?: string;
}

export default function GradientText({ children, className }: GradientTextProps) {
  return <span className={cn('gradient-text', className)}>{children}</span>;
}
