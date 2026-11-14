export type Role = 'ADMIN' | 'RISK_ANALYST' | 'UNDERWRITER'

export interface UserInfo {
  username: string
  roles: Role[]
}

export interface LoginResponse {
  accessToken: string
  tokenType: string
  expiresInSeconds: number
  user: UserInfo
}

export interface OverviewResponse {
  totalApplications: number
  defaultCount: number
  nonDefaultCount: number
  nplRate: number
  nplPercent: string
  averageLoanAmount: number
  medianLoanAmount: number
  averageIncome: number
  medianIncome: number
  averageAnnuity: number
  averageCreditToIncomeRatio: number
  def30SocialCircleRate: number
  def60SocialCircleRate: number
  modelValAuc: number | null
  baselineValAuc: number | null
  riskDistribution: {
    low: number
    medium: number
    high: number
  }
  scoredSampleSize: number
}

export interface SegmentBucket {
  label: string
  totalCount: number
  defaultCount: number
  defaultRate: number
  defaultPercent: string
}

export interface SegmentsResponse {
  byOccupation: SegmentBucket[]
  byIncomeType: SegmentBucket[]
  byEducation: SegmentBucket[]
  byExtSource2: SegmentBucket[]
}

export interface RiskDriver {
  feature: string
  featureValue: string | number | null
  shapValue: number | null
  effect: string
  raisesRisk: boolean
}

export interface ScoreResponse {
  applicationId: string | number | null
  defaultProbability: number
  riskTier: 'LOW' | 'MEDIUM' | 'HIGH' | string
  action: string
  tierDescription?: string
  topRiskDrivers: RiskDriver[]
}

export interface ApplicationResponse {
  applicationId: number
  predictedPd: number
  riskTier: string
  actionRecommendation: string
  income?: number | null
  creditAmount?: number | null
  occupation?: string | null
  education?: string | null
  scoreSource?: string | null
}

export type ApplicantPayload = Record<string, string | number | null>

export interface RegionSlice {
  regionCode: string
  regionRating: number
  applicationCount: number
  highCount: number
  mediumCount: number
  lowCount: number
  highRate: number
  averagePd: number
  averageUnemployment: number
  averageGdpGrowth: number
  averageCpiYoy: number
}

export interface RatingSlice {
  regionRating: number
  applicationCount: number
  highCount: number
  highRate: number
  averagePd: number
  averageUnemployment: number
  averageGdpGrowth: number
}

export interface MacroBucket {
  label: string
  binStart: number
  binEnd: number
  applicationCount: number
  highCount: number
  highRate: number
  averagePd: number
  averageMacroValue: number
}

export interface RegionsResponse {
  scoredSampleSize: number
  matchedWithMacro: number
  joinKey: string
  note: string
  byRegionRating: RatingSlice[]
  topRegions: RegionSlice[]
  byUnemploymentQuintile: MacroBucket[]
  byGdpQuintile: MacroBucket[]
  insights: {
    highestRiskRating: string
    highestRiskRatingHighRate: number
    highestUnemploymentBucket: string
    highestUnemploymentBucketHighRate: number
    correlationHint: number
  }
}

export interface MacroStressRequest {
  unemploymentDeltaPp: number
  cpiShockPct: number
  regionRating?: number | null
  regionCode?: string | null
}

export interface PipelineSource {
  table: string
  sourceId: string
  status: string
  rows: number
  sizeBytes: number
  seconds: number
}

export interface PipelineCleaningRow {
  table: string
  rowsIn: number
  rowsOut: number
  quarantined: number
  exactDuplicatesRemoved: number
}

export interface PipelineResponse {
  datasetLabel: string
  startedAt: string | null
  finishedAt: string | null
  fixtureRun: boolean
  note: string
  bronzeRows: number
  bronzeBytes: number
  silverRows: number
  goldCustomers: number
  goldLoans: number
  trainingLabels: number
  featureCount: number
  checksPassed: number
  checksTotal: number
  stages: { stage: string; seconds: number }[]
  sources: PipelineSource[]
  cleaning: PipelineCleaningRow[]
  goldTables: { name: string; rows: number }[]
  featureGroups: { name: string; count: number }[]
  syntheticFeatures: string[]
  checks: { name: string; passed: boolean }[]
  stream: {
    sink: string
    eventsSent: number
    eventsPerSec: number
    microBatches: number
    inputRows: number
    quarantinedRows: number
    duplicatesDropped: number
    lateRowsDropped: number
    eventsWritten: number
    snapshotCustomers: number
  }
  lake: { layer: string; filesUploaded: number; bytesUploaded: number; seconds: number }[]
}

export interface MacroStressResponse {
  unemploymentDeltaPp: number
  cpiShockPct: number
  appliedRegionRatingFilter: number | null
  appliedRegionCodeFilter: string | null
  applicantsInScope: number
  baselineTiers: { low: number; medium: number; high: number }
  stressedTiers: { low: number; medium: number; high: number }
  baselineHighRate: number
  stressedHighRate: number
  baselineAveragePd: number
  stressedAveragePd: number
  highTierDelta: number
  methodology: string
}
