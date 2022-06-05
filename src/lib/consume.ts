import {Logger} from '@open-mail-archive/logger';
import {JobData} from '@open-mail-archive/rabbitmq-helper';
import {
  AttachmentQueue,
  Attachment,
  AttachmentPayload,
} from '@open-mail-archive/types';
import {Message} from 'amqplib';

import {InvalidMessageError} from './errors';

/**
 * Consumer function that parses the message received from the RabbitMQ queue and executes the
 * required job.
 * @param {Message | null} message the message received from RabbitMQ
 */
export async function consume(message: Message | null) {
  const payload = parse(message);
  const attachment = Attachment.fromPayload(payload.data);
  switch (payload.action) {
    case 'INSERT':
      return;
    case 'DELETE':
      return deleteHandler(attachment);
    case 'UPDATE':
      return;
  }
}

/**
 * Extract the Job Data from the received message.
 * @param {Message | null} message the received message from the queue.
 * @return {JobData<EmailPayload>} the parsed job data
 * @throws {InvalidMessageError} if the message is empty
 */
function parse(message: Message | null) {
  if (message === null) {
    Logger.Instance.error({
      trace: 'AttachmentsWorker::consume::parse',
      message: `The RabbitMQ message in the ${AttachmentQueue} queue was empty!`,
    });
    throw new InvalidMessageError();
  }
  Logger.Instance.info({
    trace: 'AttachmentsWorker::consume::parse',
    message: `Parsing received message in the ${AttachmentQueue} queue.`,
  });
  const payload: JobData<AttachmentPayload> = JSON.parse(
    message.content.toString(),
  );
  Logger.Instance.debug({
    trace: 'AttachmentsWorker::consume::parse',
    message: `Message parsed.`,
    data: payload,
  });

  return payload;
}

/**
 * Handler for the delete operation. Removes the file from the storage backend.
 * @param {Attachment} attachment the data from the message payload
 */
async function deleteHandler(attachment: Attachment) {
  Logger.Instance.info({
    trace: 'AttachmentsWorker::consume::delete',
    message: `Starting job: Delete attachment from the storage backend ${attachment.checksum}.`,
  });
  await attachment.removeFromStorage();
  Logger.Instance.info({
    trace: 'AttachmentsWorker::consume::delete',
    message: `Finished job: Delete attachment from the storage backend ${attachment.checksum}.`,
  });
}
