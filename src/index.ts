import {RabbitMqHelper} from '@open-mail-archive/rabbitmq-helper';
import {AttachmentQueue} from '@open-mail-archive/types';
import {consume} from './lib/consume';

await RabbitMqHelper.init();
await RabbitMqHelper.consume(AttachmentQueue, consume);
