package mysql

const (
	maintenanceStageConnect             = "connect"
	maintenanceStageAcquireLock         = "acquire_lock"
	maintenanceStageReleaseLock         = "release_lock"
	maintenanceStageOperationAndRelease = "operation_and_release"
)
