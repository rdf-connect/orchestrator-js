import winston, { format, Logger } from 'winston'

// const PROCESSOR_NAME = 'template-processor-ts'

const DEBUG_ENV = (process.env.DEBUG || '').toLowerCase()
const LOG_LEVEL_ENV = process.env.LOG_LEVEL
function getLogLevelFor(name?: string) {
  if (LOG_LEVEL_ENV) return LOG_LEVEL_ENV
  if (DEBUG_ENV === '*' || DEBUG_ENV === "rdfc") return 'debug'
  if (
    name !== undefined &&
    DEBUG_ENV &&
    DEBUG_ENV.includes(name.toLowerCase())
  ) {
    return 'debug'
  } else {
    return 'info'
  }
}

const classLoggers = new WeakMap<Constructor, Logger>()
const stringLoggers = new Map<string, Logger>()

export function getLoggerFor(loggable: string | Instance): Logger {
  let logger: Logger
  if (typeof loggable === 'string') {
    if (stringLoggers.has(loggable)) {
      logger = stringLoggers.get(loggable)!
    } else {
      logger = createLogger(loggable)
      stringLoggers.set(loggable, logger)
    }
  } else {
    const { constructor } = loggable
    if (classLoggers.has(constructor)) {
      logger = classLoggers.get(constructor)!
    } else {
      logger = createLogger(constructor.name)
      classLoggers.set(constructor, logger)
    }
  }
  return logger
}

function createLogger(label: string): Logger {
  return winston.createLogger({
    level: getLogLevelFor(label),
    format: format.combine(
      format.label({ label }),
      format.colorize(),
      format.timestamp(),
      format.metadata({
        fillExcept: ['level', 'timestamp', 'label', 'message'],
      }),
      format.printf(
        ({
          level: levelInner,
          message,
          label: labelInner,
          timestamp,
        }): string => `${timestamp} [${labelInner}] ${levelInner}: ${message}`,
      ),
    ),
    transports: [new winston.transports.Console()],
  })
}

/**
 * Any class constructor.
 */
interface Constructor {
  name: string
}

/**
 * Any class instance.
 */
interface Instance {
  constructor: Constructor
}
