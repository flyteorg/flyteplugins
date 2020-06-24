package sagemaker

import (
	"fmt"
	"sort"

	"github.com/Masterminds/semver"
	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"
	. "github.com/aws/amazon-sagemaker-operator-for-k8s/controllers/controllertest"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	sagemakerSpec "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"
	"github.com/pkg/errors"

	"github.com/golang/protobuf/proto"
)

// Convert SparkJob ApplicationType to Operator CRD ApplicationType
func getAPIHyperParameterTuningJobStrategyType(
	strategyType sagemakerSpec.HPOJobConfig_HyperparameterTuningStrategy) commonv1.HyperParameterTuningJobStrategyType {

	switch strategyType {
	case sagemakerSpec.HPOJobConfig_BAYESIAN:
		return "Bayesian"
	}
	return "Bayesian"
}

func getAPIScalingType(scalingType sagemakerSpec.HyperparameterScalingType) commonv1.HyperParameterScalingType {
	switch scalingType {
	case sagemakerSpec.HyperparameterScalingType_AUTO:
		return AutoSageMakerAPIHyperParameterScalingType
	case sagemakerSpec.HyperparameterScalingType_LINEAR:
		return LinearSageMakerAPIHyperParameterScalingType
	case sagemakerSpec.HyperparameterScalingType_LOGARITHMIC:
		return LogarithmicSageMakerAPIHyperParameterScalingType
	case sagemakerSpec.HyperparameterScalingType_REVERSELOGARITHMIC:
		return ReverseLogarithmicSageMakerAPIHyperParameterScalingType
	}
	return AutoSageMakerAPIHyperParameterScalingType
}

func getAPIHyperparameterTuningObjectiveType(
	objectiveType sagemakerSpec.HyperparameterTuningObjective_HyperparameterTuningObjectiveType) commonv1.HyperParameterTuningJobObjectiveType {

	switch objectiveType {
	case sagemakerSpec.HyperparameterTuningObjective_MINIMIZE:
		return MinimizeSageMakerAPIHyperParameterTuningJobObjectiveType
	case sagemakerSpec.HyperparameterTuningObjective_MAXIMIZE:
		return MaximizeSageMakerAPIHyperParameterTuningJobObjectiveType
	}
	return MinimizeSageMakerAPIHyperParameterTuningJobObjectiveType
}

func getAPITrainingInputMode(trainingInputMode sagemakerSpec.InputMode) commonv1.TrainingInputMode {
	switch trainingInputMode {
	case sagemakerSpec.InputMode_FILE:
		return FileSageMakerAPITrainingInputMode
	case sagemakerSpec.InputMode_PIPE:
		return PipeSageMakerAPITrainingInputMode
	}
	return FileSageMakerAPITrainingInputMode
}

func getAPITrainingJobEarlyStoppingType(
	earlyStoppingType sagemakerSpec.HPOJobConfig_TrainingJobEarlyStoppingType) commonv1.TrainingJobEarlyStoppingType {

	switch earlyStoppingType {
	case sagemakerSpec.HPOJobConfig_OFF:
		return OffSageMakerAPITrainingJobEarlyStoppingType
	case sagemakerSpec.HPOJobConfig_AUTO:
		return AutoSageMakerAPITrainingJobEarlyStoppingType
	}
	return OffSageMakerAPITrainingJobEarlyStoppingType
}

func getAPIAlgorithmName(name sagemakerSpec.AlgorithmName) string {
	switch name {
	case sagemakerSpec.AlgorithmName_CUSTOM:
		return CustomSageMakerAPIAlgorithmName
	case sagemakerSpec.AlgorithmName_XGBOOST:
		return XgboostSageMakerAPIAlgorithmName
	}
	return CustomSageMakerAPIAlgorithmName
}

func getAllVersions(cfg *config.Config, algName, region string) []string {
	allVers := make([]string, len(cfg.AlgorithmPrebuiltImages[algName][region]))
	i := 0
	for k := range cfg.AlgorithmPrebuiltImages[algName][region] {
		allVers[i] = k
		i++
	}
	return allVers
}

func trivial(s string) {
	if s == "" {
		s = "Win!!!!"
	}
	fmt.Println(s)
}

func convertRawVersToSemVers(raw []string) ([]*semver.Version, error) {

	vs := make([]*semver.Version, len(raw))
	for i, r := range raw {
		v, err := semver.NewVersion(r)
		if err != nil {
			return nil, errors.Errorf("Failed to convert version string [%v] to semantic version [%v]", r, v)
		}
		vs[i] = v
	}
	return vs, nil
}

func getLatestSemVer(cfg *config.Config, algName, region string) (string, error) {
	allVersionsRaw := getAllVersions(cfg, algName, region)
	allSemVers, err := convertRawVersToSemVers(allVersionsRaw)
	if err != nil {
		return "", errors.Wrapf(err, "Failed to get the semantic versions of algorithm:region [%v:%v]", algName, region)
	}
	sort.Sort(semver.Collection(allSemVers))
	// https://play.golang.org/p/bd7XndTKwq5
	return allSemVers[len(allSemVers)-1].String(), nil
}

func getTrainingImage(job *sagemakerSpec.TrainingJob) (string, error) {
	cfg := config.GetSagemakerConfig()
	var err error
	if specifiedAlg := job.GetAlgorithmSpecification().GetAlgorithmName(); specifiedAlg != sagemakerSpec.AlgorithmName_CUSTOM {
		// Built-in algorithm mode
		apiAlgorithmName := getAPIAlgorithmName(specifiedAlg)

		// Getting the version
		ver := job.GetAlgorithmSpecification().GetAlgorithmVersion()
		if ver == "" {
			// user didn't specify a version -> use the latest
			ver, err = getLatestSemVer(cfg, apiAlgorithmName, cfg.Region)
			if err != nil {
				return "", errors.Wrapf(err, "Failed to identify the latest version of algorithm:region [%v:%v]", apiAlgorithmName, cfg.Region)
			}
		}

		retImg := cfg.AlgorithmPrebuiltImages[apiAlgorithmName][cfg.Region][ver]
		return retImg, nil
	}
	return "custom image", errors.Errorf("Custom images are not supported yet")
}

func buildParameterRanges(hpoJobConfig *sagemakerSpec.HPOJobConfig) *commonv1.ParameterRanges {
	prMap := hpoJobConfig.GetHyperparameterRanges().GetParameterRangeMap()
	var retValue = &commonv1.ParameterRanges{
		CategoricalParameterRanges: []commonv1.CategoricalParameterRange{},
		ContinuousParameterRanges:  []commonv1.ContinuousParameterRange{},
		IntegerParameterRanges:     []commonv1.IntegerParameterRange{},
	}

	for prName, pr := range prMap {
		switch p := pr.GetParameterRangeType().(type) {
		case sagemakerSpec.ParameterRangeOneOf_CategoricalParameterRange:
			var newElem = commonv1.CategoricalParameterRange{
				Name:   ToStringPtr(prName),
				Values: pr.GetCategoricalParameterRange().GetValues(),
			}
			retValue.CategoricalParameterRanges = append(retValue.CategoricalParameterRanges, newElem)

		case sagemakerSpec.ParameterRangeOneOf_ContinuousParameterRange:
			var newElem = commonv1.ContinuousParameterRange{
				MaxValue:    ToStringPtr(fmt.Sprintf("%f", pr.GetContinuousParameterRange().GetMaxValue())),
				MinValue:    ToStringPtr(fmt.Sprintf("%f", pr.GetContinuousParameterRange().GetMinValue())),
				Name:        ToStringPtr(prName),
				ScalingType: getAPIScalingType(pr.GetContinuousParameterRange().GetScalingType()),
			}
			retValue.ContinuousParameterRanges = append(retValue.ContinuousParameterRanges, newElem)

		case sagemakerSpec.ParameterRangeOneOf_IntegerParameterRange:
			var newElem = commonv1.IntegerParameterRange{
				MaxValue:    ToStringPtr(fmt.Sprintf("%f", pr.GetContinuousParameterRange().GetMaxValue())),
				MinValue:    ToStringPtr(fmt.Sprintf("%f", pr.GetContinuousParameterRange().GetMinValue())),
				Name:        ToStringPtr(prName),
				ScalingType: getAPIScalingType(pr.GetContinuousParameterRange().GetScalingType()),
			}
			retValue.IntegerParameterRanges = append(retValue.IntegerParameterRanges, newElem)
		}
	}

	return retValue
}

func convertHPOJobConfigToSpecType(hpoJobConfigLiteral *core.Literal) (*sagemakerSpec.HPOJobConfig, error) {
	var retValue = &sagemakerSpec.HPOJobConfig{}
	hpoJobConfigByteArray := hpoJobConfigLiteral.GetScalar().GetBinary().GetValue()
	err := proto.Unmarshal(hpoJobConfigByteArray, retValue)
	if err != nil {
		return nil, errors.Errorf("HPO Job Config Literal in input cannot be unmarshalled into spec type")
	}
	return retValue, nil
}

func convertStaticHyperparamsLiteralToSpecType(hyperparamLiteral *core.Literal) ([]*commonv1.KeyValuePair, error) {
	var retValue []*commonv1.KeyValuePair
	hyperFields := hyperparamLiteral.GetScalar().GetGeneric().GetFields()
	for k, v := range hyperFields {
		var newElem = commonv1.KeyValuePair{
			Name:  k,
			Value: v.GetStringValue(),
		}
		retValue = append(retValue, &newElem)
	}
	return retValue, nil
}

func convertStoppingConditionToSpecType(stoppingConditionLiteral *core.Literal) (*sagemakerSpec.StoppingCondition, error) {
	var retValue sagemakerSpec.StoppingCondition
	bytearray := stoppingConditionLiteral.GetScalar().GetBinary().GetValue()
	err := proto.Unmarshal(bytearray, &retValue)
	if err != nil {
		return nil, errors.Errorf("StoppingCondition Literal in input cannot be unmarshalled into spec type")
	}
	return &retValue, nil
}
