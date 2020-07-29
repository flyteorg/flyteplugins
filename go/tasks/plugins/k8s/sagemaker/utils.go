package sagemaker

import (
	"context"
	"fmt"
	"sort"

	"github.com/lyft/flytestdlib/logger"

	"github.com/Masterminds/semver"
	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"
	awssagemaker "github.com/aws/amazon-sagemaker-operator-for-k8s/controllers/controllertest"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	sagemakerSpec "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
	"github.com/lyft/flyteplugins/go/tasks/plugins/k8s/sagemaker/config"
	"github.com/pkg/errors"

	"github.com/golang/protobuf/proto"
)

func getAPIHyperParameterTuningJobStrategyType(
	strategyType sagemakerSpec.HyperparameterTuningStrategy_Value) commonv1.HyperParameterTuningJobStrategyType {

	switch strategyType {
	case sagemakerSpec.HyperparameterTuningStrategy_BAYESIAN:
		return BayesianSageMakerAPIHyperParameterTuningJobStrategyType
	}
	return RandomSageMakerAPIHyperParameterTuningJobStrategyType
}

func getAPIScalingType(scalingType sagemakerSpec.HyperparameterScalingType_Value) commonv1.HyperParameterScalingType {
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
	objectiveType sagemakerSpec.HyperparameterTuningObjectiveType_Value) commonv1.HyperParameterTuningJobObjectiveType {

	switch objectiveType {
	case sagemakerSpec.HyperparameterTuningObjectiveType_MINIMIZE:
		return MinimizeSageMakerAPIHyperParameterTuningJobObjectiveType
	case sagemakerSpec.HyperparameterTuningObjectiveType_MAXIMIZE:
		return MaximizeSageMakerAPIHyperParameterTuningJobObjectiveType
	}
	return MinimizeSageMakerAPIHyperParameterTuningJobObjectiveType
}

func getAPITrainingInputMode(trainingInputMode sagemakerSpec.InputMode_Value) commonv1.TrainingInputMode {
	switch trainingInputMode {
	case sagemakerSpec.InputMode_FILE:
		return FileSageMakerAPITrainingInputMode
	case sagemakerSpec.InputMode_PIPE:
		return PipeSageMakerAPITrainingInputMode
	}
	return FileSageMakerAPITrainingInputMode
}

func getAPITrainingJobEarlyStoppingType(
	earlyStoppingType sagemakerSpec.TrainingJobEarlyStoppingType_Value) commonv1.TrainingJobEarlyStoppingType {

	switch earlyStoppingType {
	case sagemakerSpec.TrainingJobEarlyStoppingType_OFF:
		return OffSageMakerAPITrainingJobEarlyStoppingType
	case sagemakerSpec.TrainingJobEarlyStoppingType_AUTO:
		return AutoSageMakerAPITrainingJobEarlyStoppingType
	}
	return OffSageMakerAPITrainingJobEarlyStoppingType
}

func getAPIAlgorithmName(name sagemakerSpec.AlgorithmName_Value) string {
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

func buildParameterRanges(hpoJobConfig *sagemakerSpec.HyperparameterTuningJobConfig) *commonv1.ParameterRanges {
	prMap := hpoJobConfig.GetHyperparameterRanges().GetParameterRangeMap()
	var retValue = &commonv1.ParameterRanges{
		CategoricalParameterRanges: []commonv1.CategoricalParameterRange{},
		ContinuousParameterRanges:  []commonv1.ContinuousParameterRange{},
		IntegerParameterRanges:     []commonv1.IntegerParameterRange{},
	}

	for prName, pr := range prMap {
		switch pr.GetParameterRangeType().(type) {
		case *sagemakerSpec.ParameterRangeOneOf_CategoricalParameterRange:
			var newElem = commonv1.CategoricalParameterRange{
				Name:   awssagemaker.ToStringPtr(prName),
				Values: pr.GetCategoricalParameterRange().GetValues(),
			}
			retValue.CategoricalParameterRanges = append(retValue.CategoricalParameterRanges, newElem)

		case *sagemakerSpec.ParameterRangeOneOf_ContinuousParameterRange:
			var newElem = commonv1.ContinuousParameterRange{
				MaxValue:    awssagemaker.ToStringPtr(fmt.Sprintf("%f", pr.GetContinuousParameterRange().GetMaxValue())),
				MinValue:    awssagemaker.ToStringPtr(fmt.Sprintf("%f", pr.GetContinuousParameterRange().GetMinValue())),
				Name:        awssagemaker.ToStringPtr(prName),
				ScalingType: getAPIScalingType(pr.GetContinuousParameterRange().GetScalingType()),
			}
			retValue.ContinuousParameterRanges = append(retValue.ContinuousParameterRanges, newElem)

		case *sagemakerSpec.ParameterRangeOneOf_IntegerParameterRange:
			var newElem = commonv1.IntegerParameterRange{
				MaxValue:    awssagemaker.ToStringPtr(fmt.Sprintf("%d", pr.GetIntegerParameterRange().GetMaxValue())),
				MinValue:    awssagemaker.ToStringPtr(fmt.Sprintf("%d", pr.GetIntegerParameterRange().GetMinValue())),
				Name:        awssagemaker.ToStringPtr(prName),
				ScalingType: getAPIScalingType(pr.GetContinuousParameterRange().GetScalingType()),
			}
			retValue.IntegerParameterRanges = append(retValue.IntegerParameterRanges, newElem)
		}
	}

	return retValue
}

func convertHyperparameterTuningJobConfigToSpecType(hpoJobConfigLiteral *core.Literal) (*sagemakerSpec.HyperparameterTuningJobConfig, error) {
	var retValue = &sagemakerSpec.HyperparameterTuningJobConfig{}
	hpoJobConfigByteArray := hpoJobConfigLiteral.GetScalar().GetBinary().GetValue()
	err := proto.Unmarshal(hpoJobConfigByteArray, retValue)
	if err != nil {
		return nil, errors.Errorf("Hyperparameter Tuning Job Config Literal in input cannot be unmarshalled into spec type")
	}
	return retValue, nil
}

func convertStaticHyperparamsLiteralToSpecType(hyperparamLiteral *core.Literal) ([]*commonv1.KeyValuePair, error) {
	var retValue []*commonv1.KeyValuePair
	hyperFields := hyperparamLiteral.GetScalar().GetGeneric().GetFields()
	if hyperFields == nil {
		return nil, errors.Errorf("Failed to get the static hyperparameters field from the literal")
	}
	for k, v := range hyperFields {
		var newElem = commonv1.KeyValuePair{
			Name:  k,
			Value: v.GetStringValue(),
		}
		retValue = append(retValue, &newElem)
	}
	return retValue, nil
}

func ToStringPtr(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}

func ToInt64Ptr(i int64) *int64 {
	if i == 0 {
		return nil
	}
	return &i
}

func ToIntPtr(i int) *int {
	if i == 0 {
		return nil
	}
	return &i
}

func ToFloat64Ptr(f float64) *float64 {
	if f == 0 {
		return nil
	}
	return &f
}

func createOutputLiteralMap(tk *core.TaskTemplate, outputPath string) *core.LiteralMap {
	op := &core.LiteralMap{}
	for k := range tk.Interface.Outputs.Variables {
		// if v != core.LiteralType_Blob{}
		op.Literals = make(map[string]*core.Literal)
		op.Literals[k] = &core.Literal{
			Value: &core.Literal_Scalar{
				Scalar: &core.Scalar{
					Value: &core.Scalar_Blob{
						Blob: &core.Blob{
							Metadata: &core.BlobMetadata{
								Type: &core.BlobType{Dimensionality: core.BlobType_SINGLE},
							},
							Uri: outputPath,
						},
					},
				},
			},
		}
	}
	return op
}

func deleteConflictingStaticHyperparameters(
	ctx context.Context,
	staticHPs []*commonv1.KeyValuePair,
	tunableHPMap map[string]*sagemakerSpec.ParameterRangeOneOf) []*commonv1.KeyValuePair {

	w := 0 // write position
	finalStaticHPs := make([]*commonv1.KeyValuePair, len(staticHPs))

	for _, hp := range staticHPs {
		if _, found := tunableHPMap[hp.Name]; !found {
			finalStaticHPs[w] = hp
			w++
		} else {
			logger.Infof(ctx,
				"Static hyperparameter [%v] is removed because the same hyperparameter can be found in the map of tunable hyperparameters", hp.Name)
		}
	}
	return finalStaticHPs[:w]
}
