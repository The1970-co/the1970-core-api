import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { ROLES_KEY } from "./roles.decorator";

@Injectable()
export class RolesGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const requiredRoles =
      this.reflector.getAllAndOverride<string[]>(ROLES_KEY, [
        context.getHandler(),
        context.getClass(),
      ]) || [];

    if (!requiredRoles.length) {
      return true;
    }

    const req = context.switchToHttp().getRequest();
    const user = req.user;

    const required = requiredRoles.map((r) => String(r).toLowerCase());

    const userRoles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ]
      .map((r) => String(r || "").toLowerCase())
      .filter(Boolean);

    if (!userRoles.length) {
      throw new ForbiddenException("No role found");
    }

    const ok = userRoles.some((role) => required.includes(role));

    if (!ok) {
      throw new ForbiddenException("Bạn không có quyền truy cập");
    }

    return true;
  }
}