import { Injectable } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";

@Injectable()
export class MobileAuthGuard extends JwtGuard {}