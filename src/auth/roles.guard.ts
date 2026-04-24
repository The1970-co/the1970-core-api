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

    if (!user?.role) {
      throw new ForbiddenException("No role found");
    }

    if (!requiredRoles.includes(user.role)) {
      throw new ForbiddenException("Bạn không có quyền truy cập");
    }

    return true;
  }
}