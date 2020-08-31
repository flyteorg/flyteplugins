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
	FlyteSageMakerCmdKey          string = "__FLYTE_SAGEMAKER_CMD__"
	FlyteSageMakerEnvVarKeyPrefix string = "__FLYTE_ENV_VAR_"
	FlyteSageMakerCmdArgKeyPrefix string = "__FLYTE_CMD_ARG_"
	FlyteSageMakerKeySuffix       string = "__"
	FlytesageMakerCmdKeyPrefix    string = "__FLYTE_CMD_"
)

const (
	TrainingJobOutputPathSubDir    = "training_outputs"
	HyperparameterOutputPathSubDir = "hyperparameter_tuning_outputs"
)

const (
	CustomTrainingCmdArgSeparator = "+"
)
