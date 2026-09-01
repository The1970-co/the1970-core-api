import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as jwt from "jsonwebtoken";

@Injectable()
export class CustomerJwtGuard implements CanActivate {
  constructor(private readonly prisma: PrismaService) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    const authorization = String(req.headers.authorization || "");
    if (!authorization.startsWith("Bearer ")) throw new UnauthorizedException("Missing customer token");

    const token = authorization.slice(7).trim();
    let payload: any;
    try {
      payload = jwt.verify(token, process.env.CUSTOMER_JWT_SECRET || process.env.JWT_SECRET || "dev-customer-secret");
    } catch {
      throw new UnauthorizedException("Invalid customer token");
    }

    if (payload?.type !== "customer" || !payload?.sub || !payload?.sid) {
      throw new UnauthorizedException("Invalid customer token");
    }

    const session = await this.prisma.customerSession.findUnique({
      where: { id: String(payload.sid) },
      include: { account: { include: { customer: true } } },
    });

    if (!session || session.revokedAt || session.expiresAt <= new Date()) {
      throw new UnauthorizedException("Customer session expired");
    }
    if (!session.account?.isActive || Number(payload.sv || 0) !== Number(session.account.sessionVersion || 1)) {
      throw new UnauthorizedException("Customer account inactive");
    }

    req.customer = {
      accountId: session.account.id,
      customerId: session.account.customerId,
      sessionId: session.id,
      phone: session.account.phone,
      email: session.account.email,
      fullName: session.account.customer?.fullName || "",
      type: "customer",
    };
    return true;
  }
}
