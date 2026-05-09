import type { ButtonHTMLAttributes, ReactNode } from "react"
import { Icon } from "./Icon"

type Variant = "default" | "primary" | "ghost" | "danger"
type Size = "sm" | "md" | "lg"

interface ButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "size"> {
  variant?: Variant
  size?: Size
  icon?: string
  children?: ReactNode
}

export function Button({
  variant = "default",
  size = "md",
  icon,
  children,
  className,
  ...rest
}: ButtonProps) {
  const cls = [
    "btn",
    variant !== "default" ? variant : "",
    size !== "md" ? size : "",
    className ?? "",
  ]
    .filter(Boolean)
    .join(" ")
  return (
    <button className={cls} {...rest}>
      {icon ? <Icon name={icon} size={14} /> : null}
      {children}
    </button>
  )
}

interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  icon: string
  active?: boolean
  size?: number
}

export function IconButton({
  icon,
  active,
  size = 16,
  className,
  ...rest
}: IconButtonProps) {
  const cls = ["icon-btn", active ? "active" : "", className ?? ""].filter(Boolean).join(" ")
  return (
    <button className={cls} {...rest}>
      <Icon name={icon} size={size} />
    </button>
  )
}
