import { Injectable, MessageEvent } from "@nestjs/common";
import { Observable, Subject } from "rxjs";

type InboxEvent = {
  type:
    | "conversation.created"
    | "conversation.updated"
    | "message.created"
    | "conversation.assigned"
    | "conversation.tagged"
    | "conversation.note_created"
    | "conversation.quick_order_created"
    | "conversation.quick_order_updated"
    | "conversation.quick_order_cancelled"
    | "conversation.quick_order_deleted";
  payload: any;
};

@Injectable()
export class OmniInboxRealtimeService {
  private readonly stream$ = new Subject<MessageEvent>();

  emit(event: InboxEvent) {
    this.stream$.next({
      type: event.type,
      data: event.payload,
    });
  }

  stream(): Observable<MessageEvent> {
    return this.stream$.asObservable();
  }
}
