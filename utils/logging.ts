import * as winston from "winston";
import ecsFormat from "@elastic/ecs-winston-format";

export const logger = winston.createLogger({
  level: "info",
  format: ecsFormat(),
  transports: [new winston.transports.Console()],
});
