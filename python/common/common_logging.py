import datetime as dt
import logging
import logging.config
import os
import thread

__prefix__ = 'AQSRestClient'
__pid__ = os.getpid()
__loggers__ = []


class CommonLoggingFormatter(logging.Formatter):
    converter = dt.datetime.fromtimestamp

    def format_time(self, record, date_format=None):
        ct = self.converter(record.created)
        s = ct.strftime(date_format) if date_format else ct.strftime("%Y-%m-%d %H:%M:%S")
        s += ",%03d [%d-%d]" % (record.msecs, __pid__, thread.get_ident())
        return s


class CommonLogging(object):
    @staticmethod
    def get_logger(name=None):
        if name is None:
            return logging.getLogger(__prefix__)
        if name.startswith(__prefix__):
            return logging.getLogger(__prefix__)
        logger = logging.getLogger(__prefix__ + '.' + name)
        __loggers__.append(logger)
        return logger

    @staticmethod
    def configure(log_filename=None, log_file_mode='a', log_file_level=logging.INFO,
                  log_console=True, log_console_level=logging.DEBUG):

        verbose_format = '%(asctime)s [%(process)d-%(thread)d] %(levelname)s %(name)s : %(message)s'
        simple_format = '%(levelname)s %(message)s'

        log_config = {
            'version': 1,
            'formatters': {
                'simple': {'format': simple_format},
                'verbose': {'format': verbose_format}
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler', 'formatter': 'verbose', 'level': log_console_level
                },
            },
            'loggers': {
                # '':{'handlers':['console'], 'propagate': False, 'level':logging.DEBUG},
                __prefix__: {'handlers': ['console'], 'qualname': __prefix__, 'propagate': True,
                             'level': logging.DEBUG},
            },
            # 'root':{'handlers':('console', 'file'), 'level':logging.DEBUG},
        }

        if log_filename is not None:
            log_config['handlers']['file'] = {
                'class': 'logging.FileHandler', 'filename': log_filename, 'mode': log_file_mode,
                'formatter': 'verbose', 'level': log_file_level
            }
            # logConfig['handlers']['file'] = {
            #    'class':'logging.handlers.RotatingFileHandler', 'filename':logFilename,
            #    'formatter':'verbose', 'level':logFileLevel, 'maxBytes' : 1024, 'backupCount' : 3,
            # }
            for key, handle in log_config['loggers'].items():
                handle['handlers'].append('file')

        logging.config.dictConfig(log_config)

    @staticmethod
    def shutdown():
        logging.shutdown()
