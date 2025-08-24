import { PrefixCallback } from 'n3'
import path from 'path'
import { pathToFileURL } from 'url'
import winston, { format, Logger } from 'winston'

const urlBase = new URL("./", pathToFileURL(path.resolve('./pipeline.ttl')));

const prefixes: { prefix: string; node: string }[] = [
    {
        prefix: '',
        node: urlBase.toString(),
    },
]

export function setPipelineFile(path: URL) {
    while (prefixes.pop()) {
        //clearing
    }
    const dirUrl = new URL("./", path).toString();
    prefixes.push({ prefix: '', node: dirUrl })
}

export const prefixFound: PrefixCallback = (prefix, node) => {
    prefixes.push({ prefix, node: node.value })
}

const DEBUG_ENV = (process.env.DEBUG || '').toLowerCase()
const LOG_LEVEL_ENV = process.env.LOG_LEVEL
function collapse(node: string): string[] {
    const out = [new URL(node, urlBase).toString(), node]
    for (const pref of prefixes) {
        if (node.startsWith(pref.node)) {
            out.push(node.replace(pref.node, pref.prefix + ':'))
        }
    }
    return out
}

function getLogLevelFor(name: string[]) {
    if (LOG_LEVEL_ENV) return LOG_LEVEL_ENV
    if (DEBUG_ENV === '*' || DEBUG_ENV === 'rdfc') return 'debug'

    const names = name.flatMap(collapse).map((x) => x.toLowerCase())
    if (
        name !== undefined &&
        DEBUG_ENV &&
        names.some((name) => DEBUG_ENV.includes(name.toLowerCase()))
    ) {
        return 'debug'
    } else {
        return 'info'
    }
}

const loggers: { logger: Logger; names: string[]; aliases: string[] }[] = []
const stringLoggers = new Map<string, Logger>()

function getSuperClassNames(instance: Instance): string[] {
    const out: string[] = []

    let current = instance

    while (current.constructor.name !== 'Object') {
        out.push(current.constructor.name)
        const next = Object.getPrototypeOf(current)
        if (next == current) break
        current = next
    }

    return out
}

export function getLoggerFor(
    loggable: (string | Instance)[],
    xs: (string | Instance)[] = [],
): Logger {
    let logger: Logger
    const aliases = xs.flatMap((x) =>
        typeof x === 'string' ? [x] : getSuperClassNames(x),
    )
    const names = loggable.map((x) => {
        if (typeof x === 'string') {
            aliases.push(x)
            return x
        }
        aliases.push(...getSuperClassNames(x))
        return x.constructor.name
    })
    const id = names.join('/')
    if (stringLoggers.has(id)) {
        logger = stringLoggers.get(id)!
    } else {
        logger = createLogger(names, aliases)
        stringLoggers.set(id, logger)
        loggers.push({ logger, names, aliases })
    }
    return logger
}

export function reevaluteLevels() {
    for (const logger of loggers) {
        const level = getLogLevelFor(logger.aliases)
        logger.logger.clear()
        logger.logger.level = level
        logger.logger.add(
            new winston.transports.Console({
                level: level,
                format: formatter(logger.names),
            }),
        )
        // logger.logger.format = formatter(logger.names)
    }
}

function formatter(names: string[]) {
    return format.combine(
        // This should be done better
        format.label({
            label: names.map((x) => collapse(x).pop()!).join(', '),
        }),
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
            }): string =>
                `${timestamp} [${labelInner}] ${levelInner}: ${message}`,
        ),
    )
}
function createLogger(names: string[], aliases: string[]): Logger {
    return winston.createLogger({
        transports: [
            new winston.transports.Console({
                level: getLogLevelFor(aliases),
                format: formatter(names),
            }),
        ],
    })
}

interface Constructor {
    name: string
}

interface Instance {
    constructor: Constructor
}
