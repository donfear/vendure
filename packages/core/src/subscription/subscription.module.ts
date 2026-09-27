import { Module } from '@nestjs/common';

import { ConfigModule } from '../config/config.module';
import { EventBusModule } from '../event-bus/event-bus.module';

import { SubscriptionService } from './subscription.service';

@Module({
    imports: [ConfigModule, EventBusModule],
    providers: [SubscriptionService],
    exports: [SubscriptionService],
})
export class SubscriptionModule {}
