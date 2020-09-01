package sagemaker

const (
	trainingJobTaskPluginID = "sagemaker_training"
	trainingJobTaskType     = "sagemaker_training_job_task"
)

const (
	customTrainingJobTaskPluginID = "sagemaker_custom_training"
	customTrainingJobTaskType     = "sagemaker_custom_training_job_task"
)

const (
	hyperparameterTuningJobTaskPluginID = "sagemaker_hyperparameter_tuning"
	hyperparameterTuningJobTaskType     = "sagemaker_hyperparameter_tuning_job_task"
)

const (
	TEXTCSVInputContentType string = "text/csv"
)

const (
	FlyteSageMakerEnvVarKeyPrefix         string = "__FLYTE_ENV_VAR_"
	FlyteSageMakerKeySuffix               string = "__"
	FlyteSageMakerCmdKeyPrefix            string = "__FLYTE_CMD_"
	FlyteSageMakerCmdDummyValue           string = "__FLYTE_CMD_DUMMY_VALUE__"
	FlytesageMakerEnvVarKeyStatsdDisabled string = "FLYTE_STATSD_DISABLED"
)

const (
	TrainingJobOutputPathSubDir    = "training_outputs"
	HyperparameterOutputPathSubDir = "hyperparameter_tuning_outputs"
)

const (
	TrainPredefinedInputVariable                 = "train"
	ValidationPredefinedInputVariable            = "validation"
	StaticHyperparametersPredefinedInputVariable = "static_hyperparameters"
)
