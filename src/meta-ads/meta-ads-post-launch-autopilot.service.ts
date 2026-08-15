import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsInventoryAutopilotService } from './meta-ads-inventory-autopilot.service';

type AutomationLevel = 'manual' | 'semi' | 'auto';
type LaunchMode = 'EXISTING_ADSET' | 'CLONE_ADSET' | 'NEW_CAMPAIGN';
type PostState =
  | 'DISCOVERED'
  | 'WAITING'
  | 'READY'
  | 'UNMAPPED'
  | 'BLOCKED_STOCK'
  | 'DRY_RUN'
  | 'CREATED_PAUSED'
  | 'ACTIVE'
  | 'ALREADY_AD'
  | 'SKIPPED'
  | 'ERROR';

type AutoLaunchPost = {
  id: string;
  message?: string | null;
  created_time?: string | null;
  permalink_url?: string | null;
  full_picture?: string | null;
  attachments?: any;
};

function n(value: any, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

@Injectable()
export class MetaAdsPostLaunchAutopilotService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MetaAdsPostLaunchAutopilotService.name);
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  private runtimeEnabled = this.envBool('META_ADS_AUTO_LAUNCH_ENABLED', false);
  private runtimeDryRun = this.envBool('META_ADS_AUTO_LAUNCH_DRY_RUN', true);
  private runtimeLevel: AutomationLevel = this.envLevel(process.env.META_ADS_PERFORMANCE_AUTOMATION_LEVEL || 'manual');
  private waitHours = Math.max(1, this.envNumber('META_ADS_AUTO_LAUNCH_WAIT_HOURS', 48));
  private launchMode: LaunchMode = this.envLaunchMode(process.env.META_ADS_AUTO_LAUNCH_MODE || 'NEW_CAMPAIGN');
  private targetAdSetId = String(process.env.META_ADS_AUTO_LAUNCH_ADSET_ID || '').trim();
  private templateAdSetId = String(process.env.META_ADS_AUTO_LAUNCH_TEMPLATE_ADSET_ID || '').trim();
  private targetCampaignId = String(process.env.META_ADS_AUTO_LAUNCH_CAMPAIGN_ID || '').trim();
  private dailyBudget = Math.max(0, this.envNumber('META_ADS_AUTO_LAUNCH_DAILY_BUDGET', 1000000));
  private requireInventoryMatch = this.envBool('META_ADS_AUTO_LAUNCH_REQUIRE_INVENTORY_MATCH', true);
  private blockCriticalStock = this.envBool('META_ADS_AUTO_LAUNCH_BLOCK_CRITICAL_STOCK', true);
  private autoActivate = this.envBool('META_ADS_AUTO_LAUNCH_AUTO_ACTIVATE', true);
  private maxPostsPerRun = Math.max(1, Math.min(50, this.envNumber('META_ADS_AUTO_LAUNCH_MAX_POSTS_PER_RUN', 10)));
  private lastRunAt: string | null = null;
  private lastSummary: any = null;

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaAdsSyncService: MetaAdsSyncService,
    private readonly inventoryAutopilotService: MetaAdsInventoryAutopilotService,
  ) {}

  private envBool(key: string, fallback: boolean) {
    const raw = String(process.env[key] ?? '').trim().toLowerCase();
    if (!raw) return fallback;
    return ['1', 'true', 'yes', 'on'].includes(raw);
  }

  private envNumber(key: string, fallback: number) {
    const value = Number(process.env[key]);
    return Number.isFinite(value) ? value : fallback;
  }

  private envLevel(value: any): AutomationLevel {
    const v = String(value || '').toLowerCase();
    return v === 'auto' || v === 'semi' ? v : 'manual';
  }

  private envLaunchMode(value: any): LaunchMode {
    const v = String(value || '').toUpperCase();
    if (v === 'EXISTING_ADSET' || v === 'CLONE_ADSET') return v;
    return 'NEW_CAMPAIGN';
  }

  private get intervalMs() {
    return Math.max(60_000, this.envNumber('META_ADS_AUTO_LAUNCH_INTERVAL_MS', 300_000));
  }

  private runScheduledSafely(source: 'startup' | 'interval') {
    void this.runNow({ source }).catch((error: any) => {
      this.logger.error(`[$META_POST_LAUNCH_AUTOPILOT] scheduled ${source} failed: ${error?.message || error}`);
    });
  }

  async onModuleInit() {
    await this.loadPersistedConfig();
    this.restartTimer();
    if (this.runtimeEnabled) setTimeout(() => this.runScheduledSafely('startup'), 25_000);
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private restartTimer() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (!this.runtimeEnabled) return;
    this.timer = setInterval(() => this.runScheduledSafely('interval'), this.intervalMs);
  }

  async setRuntimeConfig(input: any = {}) {
    if (typeof input.enabled === 'boolean') this.runtimeEnabled = input.enabled;
    if (typeof input.dryRun === 'boolean') this.runtimeDryRun = input.dryRun;
    if (input.level) this.runtimeLevel = this.envLevel(input.level);
    if (Number.isFinite(Number(input.waitHours))) this.waitHours = Math.max(1, Number(input.waitHours));
    if (input.launchMode) this.launchMode = this.envLaunchMode(input.launchMode);
    if (typeof input.targetAdSetId === 'string') this.targetAdSetId = input.targetAdSetId.trim();
    if (typeof input.templateAdSetId === 'string') this.templateAdSetId = input.templateAdSetId.trim();
    if (typeof input.targetCampaignId === 'string') this.targetCampaignId = input.targetCampaignId.trim();
    if (Number.isFinite(Number(input.dailyBudget))) this.dailyBudget = Math.max(0, Math.round(Number(input.dailyBudget)));
    if (typeof input.requireInventoryMatch === 'boolean') this.requireInventoryMatch = input.requireInventoryMatch;
    if (typeof input.blockCriticalStock === 'boolean') this.blockCriticalStock = input.blockCriticalStock;
    if (typeof input.autoActivate === 'boolean') this.autoActivate = input.autoActivate;
    if (Number.isFinite(Number(input.maxPostsPerRun))) this.maxPostsPerRun = Math.max(1, Math.min(50, Math.round(Number(input.maxPostsPerRun))));
    this.restartTimer();
    await this.persistRuntimeConfig();
    return this.getStatus();
  }

  private async loadPersistedConfig() {
    try {
      const row = await (this.prisma as any).metaSyncLog.findFirst({
        where: { syncType: 'META_ADS_AUTO_LAUNCH_CONFIG', status: 'SUCCESS' },
        orderBy: { startedAt: 'desc' },
      });
      const c = (row?.errorJson as any)?.config || {};
      if (typeof c.enabled === 'boolean') this.runtimeEnabled = c.enabled;
      if (typeof c.dryRun === 'boolean') this.runtimeDryRun = c.dryRun;
      if (c.level) this.runtimeLevel = this.envLevel(c.level);
      if (Number.isFinite(Number(c.waitHours))) this.waitHours = Math.max(1, Number(c.waitHours));
      if (c.launchMode) this.launchMode = this.envLaunchMode(c.launchMode);
      if (typeof c.targetAdSetId === 'string') this.targetAdSetId = c.targetAdSetId;
      if (typeof c.templateAdSetId === 'string') this.templateAdSetId = c.templateAdSetId;
      if (typeof c.targetCampaignId === 'string') this.targetCampaignId = c.targetCampaignId;
      if (Number.isFinite(Number(c.dailyBudget))) this.dailyBudget = Math.max(0, Math.round(Number(c.dailyBudget)));
      if (typeof c.requireInventoryMatch === 'boolean') this.requireInventoryMatch = c.requireInventoryMatch;
      if (typeof c.blockCriticalStock === 'boolean') this.blockCriticalStock = c.blockCriticalStock;
      if (typeof c.autoActivate === 'boolean') this.autoActivate = c.autoActivate;
      if (Number.isFinite(Number(c.maxPostsPerRun))) this.maxPostsPerRun = Math.max(1, Math.min(50, Math.round(Number(c.maxPostsPerRun))));
    } catch (error: any) {
      this.logger.warn(`[AUTO_LAUNCH_CONFIG_LOAD] ${error?.message || error}`);
    }
  }

  private configObject() {
    return {
      enabled: this.runtimeEnabled,
      dryRun: this.runtimeDryRun,
      level: this.runtimeLevel,
      waitHours: this.waitHours,
      launchMode: this.launchMode,
      targetAdSetId: this.targetAdSetId,
      templateAdSetId: this.templateAdSetId,
      targetCampaignId: this.targetCampaignId,
      dailyBudget: this.dailyBudget,
      requireInventoryMatch: this.requireInventoryMatch,
      blockCriticalStock: this.blockCriticalStock,
      autoActivate: this.autoActivate,
      maxPostsPerRun: this.maxPostsPerRun,
    };
  }

  private async persistRuntimeConfig() {
    const config = this.configObject();
    try {
      await (this.prisma as any).metaSyncLog.create({
        data: {
          metaAccountId: null,
          syncType: 'META_ADS_AUTO_LAUNCH_CONFIG',
          status: 'SUCCESS',
          range: 'config',
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 0,
          scanned: 0,
          upserted: 1,
          failed: 0,
          message: 'Saved Meta Ads Auto Launch config',
          errorJson: { config },
        },
      });
    } catch (error: any) {
      this.logger.warn(`[AUTO_LAUNCH_CONFIG_SAVE] ${error?.message || error}`);
    }
  }

  getStatus() {
    return {
      ok: true,
      ...this.configObject(),
      running: this.running,
      intervalMs: this.intervalMs,
      lastRunAt: this.lastRunAt,
      lastSummary: this.lastSummary,
      rule: `Phát hiện bài Page → chờ ${this.waitHours}h → kiểm tra mã/màu + tồn kho → ${this.runtimeLevel === 'auto' ? 'tạo và bật Ad' : this.runtimeLevel === 'semi' ? 'tạo Ad PAUSED để duyệt' : 'chỉ đưa vào READY'}.`,
    };
  }

  private postPayload(row: any) {
    return (row?.errorJson || {}) as Record<string, any>;
  }

  private async loadLatestPostStates() {
    const rows = await (this.prisma as any).metaSyncLog.findMany({
      where: { syncType: 'META_ADS_AUTO_LAUNCH_POST' },
      orderBy: { startedAt: 'desc' },
      take: 5000,
    });
    const map = new Map<string, any>();
    for (const row of rows || []) {
      const payload = this.postPayload(row);
      const postId = String(payload?.postId || '').trim();
      if (postId && !map.has(postId)) map.set(postId, { ...payload, logId: row.id, at: row.startedAt });
    }
    return map;
  }

  private async writePostState(post: AutoLaunchPost, state: PostState, extra: Record<string, any> = {}) {
    const payload = {
      postId: post.id,
      state,
      message: post.message || null,
      createdTime: post.created_time || null,
      permalinkUrl: post.permalink_url || null,
      fullPicture: post.full_picture || null,
      ...extra,
    };
    try {
      await (this.prisma as any).metaSyncLog.create({
        data: {
          metaAccountId: null,
          syncType: 'META_ADS_AUTO_LAUNCH_POST',
          status: state === 'ERROR' ? 'FAILED' : 'SUCCESS',
          range: 'auto_launch',
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 0,
          scanned: 1,
          upserted: state === 'ERROR' ? 0 : 1,
          failed: state === 'ERROR' ? 1 : 0,
          message: `Auto Launch ${state}: ${post.id}`,
          errorJson: payload,
        },
      });
    } catch (error: any) {
      this.logger.warn(`[AUTO_LAUNCH_POST_LOG] ${post.id}: ${error?.message || error}`);
    }
    return payload;
  }

  private ageHours(post: AutoLaunchPost) {
    const at = post.created_time ? new Date(post.created_time).getTime() : NaN;
    if (!Number.isFinite(at)) return 0;
    return Math.max(0, (Date.now() - at) / 3_600_000);
  }

  private adName(post: AutoLaunchPost, assessment: any) {
    const first = String(post.message || '').split(/\n+/)[0].replace(/#\S+/g, '').trim();
    const productName = String(assessment?.productName || '').trim();
    const productCode = String(assessment?.productCode || '').trim();
    const color = String(assessment?.color || '').trim();

    const base =
      productName ||
      first ||
      [productCode, color].filter(Boolean).join(' ') ||
      'Page post';

    const createdDate = new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      day: 'numeric',
      month: 'numeric',
      year: 'numeric',
    }).format(new Date());

    return `${base} ${createdDate}`.replace(/\s+/g, ' ').trim().slice(0, 200);
  }

  private async assessPost(post: AutoLaunchPost) {
    const rows = await this.inventoryAutopilotService.assessAdsForScale([
      {
        id: post.id,
        metaAdId: post.id,
        name: post.message || '',
        adName: post.message || '',
        status: 'ACTIVE',
        effectiveStatus: 'ACTIVE',
      },
    ]);
    return rows?.[0] || null;
  }

  private async existingAdsForPosts(postIds: string[]) {
    if (!postIds.length) return new Map<string, any>();
    const rows = await (this.prisma as any).metaAd.findMany({
      where: { postId: { in: postIds } },
      select: { postId: true, metaAdId: true, metaAdSetId: true, metaCampaignId: true, name: true, status: true, effectiveStatus: true },
      take: 5000,
    });
    const map = new Map<string, any>();
    for (const row of rows || []) if (row?.postId && !map.has(String(row.postId))) map.set(String(row.postId), row);
    return map;
  }

  async getPosts(limit = 100) {
    const states = await this.loadLatestPostStates();
    const items = Array.from(states.values()).sort((a: any, b: any) => {
      const ta = new Date(a?.createdTime || a?.at || 0).getTime();
      const tb = new Date(b?.createdTime || b?.at || 0).getTime();
      return tb - ta;
    });
    return {
      ok: true,
      items: items.slice(0, Math.min(Math.max(Number(limit || 100), 1), 500)),
    };
  }


  async setManualMapping(input: { postId: string; productCode: string; color?: string }) {
    const postId = String(input?.postId || '').trim();
    const productCode = String(input?.productCode || '').trim().toUpperCase();
    const color = String(input?.color || '').trim();
    if (!postId) throw new Error('Thiếu postId');
    if (!productCode) throw new Error('Chưa chọn mã sản phẩm');

    const pageId = await this.metaAdsSyncService.resolvePageIdForAutoLaunch();
    const posts = await this.metaAdsSyncService.getPublishedPagePostsForAutoLaunch(pageId, 100);
    const post = posts.find((row: AutoLaunchPost) => String(row.id) === postId);
    if (!post) throw new Error('Không tìm thấy bài Page trong 100 bài đã đăng gần nhất');

    const assessment = await this.inventoryAutopilotService.assessManualProductForLaunch({
      productCode,
      color: color || undefined,
    });

    const level = String(assessment?.level || '').toUpperCase();
    if (['UNMAPPED', 'AMBIGUOUS'].includes(level)) {
      return { ok: false, postId, assessment, error: assessment?.reason || 'Mapping chưa chính xác' };
    }

    const states = await this.loadLatestPostStates();
    const previous = states.get(postId);
    const ageHours = this.ageHours(post);
    let state: PostState = ageHours >= this.waitHours ? 'READY' : 'WAITING';
    if (this.blockCriticalStock && level === 'CRITICAL') state = 'BLOCKED_STOCK';
    if (previous?.state === 'ALREADY_AD' || previous?.state === 'CREATED_PAUSED' || previous?.state === 'ACTIVE') {
      state = previous.state;
    }

    const logged = await this.writePostState(post, state, {
      ...(previous || {}),
      ageHours,
      assessment: {
        ...assessment,
        source: 'MANUAL_PRODUCT_OVERRIDE',
        manuallyConfirmed: true,
      },
      manualMapping: {
        productCode: assessment.productCode,
        color: assessment.color,
        confirmedAt: new Date().toISOString(),
      },
    });

    return { ok: true, postId, state, assessment, item: logged };
  }


  async skipPost(postId: string) {
    const pageId = await this.metaAdsSyncService.resolvePageIdForAutoLaunch();
    const posts = await this.metaAdsSyncService.getPublishedPagePostsForAutoLaunch(pageId, 100);
    const post = posts.find((x: any) => String(x.id) === String(postId));
    if (!post) throw new Error('Không tìm thấy Page post');
    return this.writePostState(post, 'SKIPPED', { reason: 'Bỏ qua thủ công' });
  }

  async runNow(options: { source?: string; dryRun?: boolean; postId?: string; force?: boolean; manualOverride?: boolean; manualProductCode?: string; manualColor?: string; discoverOnly?: boolean; scanLimit?: number } = {}) {
    if (this.running) return { ok: false, skipped: true, reason: 'Auto Launch đang chạy một phiên khác', status: this.getStatus() };
    this.running = true;
    const started = Date.now();
    const dryRun = typeof options.dryRun === 'boolean' ? options.dryRun : this.runtimeDryRun;
    try {
      const pageId = await this.metaAdsSyncService.resolvePageIdForAutoLaunch();
      const scanLimit = Math.min(100, Math.max(1, Number(options.scanLimit || 100)));
      const published = await this.metaAdsSyncService.getPublishedPagePostsForAutoLaunch(pageId, scanLimit);
      const posts = published
        .filter((post: AutoLaunchPost) => !options.postId || String(post.id) === String(options.postId))
        .slice(0, options.discoverOnly ? scanLimit : this.maxPostsPerRun);
      const states = await this.loadLatestPostStates();
      const existingAds = await this.existingAdsForPosts(posts.map((p: AutoLaunchPost) => p.id));
      const results: any[] = [];

      // “Quét bài đã đăng” chỉ phát hiện/trạng thái, tuyệt đối không tạo hoặc bật Ads.
      if (options.discoverOnly) {
        for (const post of posts) {
          const previous = states.get(post.id);
          const existing = existingAds.get(post.id);
          const ageHours = this.ageHours(post);

          if (existing) {
            const state: PostState = 'ALREADY_AD';
            const payload = {
              ageHours,
              hasAd: true,
              metaAdId: existing.metaAdId,
              metaAdSetId: existing.metaAdSetId,
              metaCampaignId: existing.metaCampaignId,
              adName: existing.name || null,
            };
            const logged = previous?.state === state ? { ...previous, ...payload } : await this.writePostState(post, state, payload);
            states.set(post.id, logged);
            results.push(logged);
            continue;
          }

          if (previous?.state === 'SKIPPED') {
            results.push({ ...previous, hasAd: false });
            continue;
          }

          let assessment: any = await this.assessPost(post);

        const manualProductCode = String(options.manualProductCode || '').trim().toUpperCase();
        const manualColor = String(options.manualColor || '').trim();
        if (manualProductCode) {
          assessment = await this.inventoryAutopilotService.assessManualProductForLaunch({
            productCode: manualProductCode,
            color: manualColor || undefined,
          });
        }
          let state: PostState;
          if (this.requireInventoryMatch && (!assessment || ['UNMAPPED', 'AMBIGUOUS'].includes(String(assessment.level || '').toUpperCase()))) {
            state = 'UNMAPPED';
          } else if (this.blockCriticalStock && String(assessment?.level || '').toUpperCase() === 'CRITICAL') {
            state = 'BLOCKED_STOCK';
          } else if (ageHours >= this.waitHours) {
            state = 'READY';
          } else {
            state = 'WAITING';
          }

          const payload = {
            ageHours,
            hasAd: false,
            readyAt: post.created_time ? new Date(new Date(post.created_time).getTime() + this.waitHours * 3_600_000).toISOString() : null,
            assessment,
          };
          const logged = previous?.state === state ? { ...previous, ...payload } : await this.writePostState(post, state, payload);
          states.set(post.id, logged);
          results.push(logged);
        }

        this.lastRunAt = new Date().toISOString();
        this.lastSummary = {
          source: options.source || 'api-discovery',
          pageId,
          discoveryOnly: true,
          scanned: posts.length,
          withAds: results.filter((x) => x.hasAd || x.state === 'ALREADY_AD').length,
          withoutAds: results.filter((x) => !(x.hasAd || x.state === 'ALREADY_AD')).length,
          ready: results.filter((x) => x.state === 'READY').length,
          waiting: results.filter((x) => x.state === 'WAITING').length,
          unmapped: results.filter((x) => x.state === 'UNMAPPED').length,
          durationMs: Date.now() - started,
        };
        return { ok: true, summary: this.lastSummary, results, status: this.getStatus() };
      }

      for (const post of posts) {
        const previous = states.get(post.id);
        if (previous?.state === 'SKIPPED' && !options.force) {
          results.push({ postId: post.id, state: 'SKIPPED' });
          continue;
        }

        const existing = existingAds.get(post.id);
        if (existing) {
          if (previous?.state !== 'ALREADY_AD' && previous?.state !== 'ACTIVE') {
            const logged = await this.writePostState(post, 'ALREADY_AD', { metaAdId: existing.metaAdId, metaAdSetId: existing.metaAdSetId, metaCampaignId: existing.metaCampaignId });
            states.set(post.id, logged);
          }
          results.push({ postId: post.id, state: 'ALREADY_AD', metaAdId: existing.metaAdId });
          continue;
        }

        if (previous?.state === 'ACTIVE') {
          results.push(previous);
          continue;
        }

        const ageHours = this.ageHours(post);
        const due = options.force === true || ageHours >= this.waitHours;
        const assessment = await this.assessPost(post);

        const inventoryUnmapped =
          !assessment || ['UNMAPPED', 'AMBIGUOUS'].includes(String(assessment.level || '').toUpperCase());

        // Auto/Scheduled vẫn bắt buộc match mã+màu nếu setting đang bật.
        // Nhưng khi người dùng bấm "Chạy bài này" thủ công, manualOverride cho phép chạy dù chưa map.
        // Bài đó sẽ không có bảo vệ tồn kho cho tới khi map được sản phẩm.
        if (this.requireInventoryMatch && inventoryUnmapped && !options.manualOverride) {
          if (previous?.state !== 'UNMAPPED') states.set(post.id, await this.writePostState(post, 'UNMAPPED', { ageHours, assessment }));
          results.push({ postId: post.id, state: 'UNMAPPED', ageHours, assessment });
          continue;
        }

        if (this.blockCriticalStock && String(assessment?.level || '').toUpperCase() === 'CRITICAL') {
          if (previous?.state !== 'BLOCKED_STOCK') states.set(post.id, await this.writePostState(post, 'BLOCKED_STOCK', { ageHours, assessment }));
          results.push({ postId: post.id, state: 'BLOCKED_STOCK', ageHours, assessment });
          continue;
        }

        if (!due) {
          if (previous?.state !== 'WAITING') states.set(post.id, await this.writePostState(post, 'WAITING', { ageHours, readyAt: post.created_time ? new Date(new Date(post.created_time).getTime() + this.waitHours * 3_600_000).toISOString() : null, assessment }));
          results.push({ postId: post.id, state: 'WAITING', ageHours, assessment });
          continue;
        }

        if (this.runtimeLevel === 'manual') {
          if (previous?.state !== 'READY') states.set(post.id, await this.writePostState(post, 'READY', { ageHours, assessment }));
          results.push({ postId: post.id, state: 'READY', ageHours, assessment });
          continue;
        }

        if (previous?.state === 'CREATED_PAUSED' && previous?.metaAdId && this.runtimeLevel === 'auto' && this.autoActivate && !dryRun) {
          await this.metaAdsSyncService.setAdStatus(String(previous.metaAdId), 'ACTIVE');
          const logged = await this.writePostState(post, 'ACTIVE', { ...previous, activatedAt: new Date().toISOString(), source: options.source || 'auto' });
          states.set(post.id, logged);
          results.push(logged);
          continue;
        }

        if (previous?.state === 'CREATED_PAUSED') {
          results.push(previous);
          continue;
        }

        if (dryRun) {
          const preview = {
            ageHours,
            assessment,
            launchMode: this.launchMode,
            targetAdSetId: this.targetAdSetId,
            templateAdSetId: this.templateAdSetId,
            targetCampaignId: this.targetCampaignId,
            dailyBudget: this.dailyBudget,
            wouldActivate: this.runtimeLevel === 'auto' && this.autoActivate,
          };
          if (previous?.state !== 'DRY_RUN') states.set(post.id, await this.writePostState(post, 'DRY_RUN', preview));
          results.push({ postId: post.id, state: 'DRY_RUN', ...preview });
          continue;
        }

        try {
          const adSet = await this.metaAdsSyncService.prepareAdSetForPagePostAutoLaunch({
            launchMode: this.launchMode,
            targetAdSetId: this.targetAdSetId,
            templateAdSetId: this.templateAdSetId,
            targetCampaignId: this.targetCampaignId,
            dailyBudget: this.dailyBudget,
            name: this.adName(post, assessment),
          });
          const creative = await this.metaAdsSyncService.createCreativeFromPagePostAutoLaunch({
            pageId,
            postId: post.id,
            name: `${this.adName(post, assessment)} · Creative`,
          });
          const ad = await this.metaAdsSyncService.createAdFromCreativeAutoLaunch({
            adSetId: String(adSet.metaAdSetId),
            creativeId: String(creative.metaCreativeId),
            name: this.adName(post, assessment),
            status: 'PAUSED',
          });

          if (this.runtimeLevel === 'auto' && this.autoActivate) {
            await this.metaAdsSyncService.setAdStatus(String(ad.metaAdId), 'ACTIVE');
            const logged = await this.writePostState(post, 'ACTIVE', {
              ageHours,
              assessment,
              metaAdId: ad.metaAdId,
              metaCreativeId: creative.metaCreativeId,
              metaAdSetId: adSet.metaAdSetId,
              metaCampaignId: adSet.metaCampaignId || null,
              launchedAt: new Date().toISOString(),
              source: options.source || 'auto',
            });
            states.set(post.id, logged);
            results.push(logged);
          } else {
            const logged = await this.writePostState(post, 'CREATED_PAUSED', {
              ageHours,
              assessment,
              metaAdId: ad.metaAdId,
              metaCreativeId: creative.metaCreativeId,
              metaAdSetId: adSet.metaAdSetId,
              metaCampaignId: adSet.metaCampaignId || null,
              createdAt: new Date().toISOString(),
              source: options.source || 'semi',
            });
            states.set(post.id, logged);
            results.push(logged);
          }
        } catch (error: any) {
          const logged = await this.writePostState(post, 'ERROR', { ageHours, assessment, error: error?.message || String(error) });
          states.set(post.id, logged);
          results.push(logged);
        }
      }

      this.lastRunAt = new Date().toISOString();
      this.lastSummary = {
        source: options.source || 'manual',
        pageId,
        scanned: posts.length,
        waiting: results.filter((x) => x.state === 'WAITING').length,
        ready: results.filter((x) => x.state === 'READY').length,
        launched: results.filter((x) => x.state === 'ACTIVE').length,
        pausedDrafts: results.filter((x) => x.state === 'CREATED_PAUSED').length,
        blocked: results.filter((x) => ['UNMAPPED', 'BLOCKED_STOCK'].includes(x.state)).length,
        errors: results.filter((x) => x.state === 'ERROR').length,
        durationMs: Date.now() - started,
      };
      return { ok: true, summary: this.lastSummary, results, status: this.getStatus() };
    } finally {
      this.running = false;
    }
  }
}
