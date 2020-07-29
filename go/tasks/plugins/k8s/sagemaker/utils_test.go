package sagemaker

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	commonv1 "github.com/aws/amazon-sagemaker-operator-for-k8s/api/v1/common"
	sagemakerSpec "github.com/lyft/flyteidl/gen/pb-go/flyteidl/plugins/sagemaker"
)

func generateMockTunableHPMap() map[string]*sagemakerSpec.ParameterRangeOneOf {
	ret := map[string]*sagemakerSpec.ParameterRangeOneOf{
		"hp1": {ParameterRangeType: &sagemakerSpec.ParameterRangeOneOf_IntegerParameterRange{
			IntegerParameterRange: &sagemakerSpec.IntegerParameterRange{
				MaxValue: 10, MinValue: 0, ScalingType: sagemakerSpec.HyperparameterScalingType_AUTO}}},
		"hp2": {ParameterRangeType: &sagemakerSpec.ParameterRangeOneOf_ContinuousParameterRange{
			ContinuousParameterRange: &sagemakerSpec.ContinuousParameterRange{
				MaxValue: 5.0, MinValue: 3.0, ScalingType: sagemakerSpec.HyperparameterScalingType_LINEAR}}},
		"hp3": {ParameterRangeType: &sagemakerSpec.ParameterRangeOneOf_CategoricalParameterRange{
			CategoricalParameterRange: &sagemakerSpec.CategoricalParameterRange{
				Values: []string{"AAA", "BBB", "CCC"}}}},
	}
	return ret
}

func generatePartiallyConflictingStaticHPs() []*commonv1.KeyValuePair {
	ret := []*commonv1.KeyValuePair{
		{Name: "hp1", Value: "100"},
		{Name: "hp4", Value: "0.5"},
		{Name: "hp3", Value: "ddd,eee"},
	}
	return ret
}

func generateTotallyConflictingStaticHPs() []*commonv1.KeyValuePair {
	ret := []*commonv1.KeyValuePair{
		{Name: "hp1", Value: "100"},
		{Name: "hp2", Value: "0.5"},
		{Name: "hp3", Value: "ddd,eee"},
	}
	return ret
}

func generateNonConflictingStaticHPs() []*commonv1.KeyValuePair {
	ret := []*commonv1.KeyValuePair{
		{Name: "hp5", Value: "100"},
		{Name: "hp4", Value: "0.5"},
		{Name: "hp7", Value: "ddd,eee"},
	}
	return ret
}

func generateMockHyperparameterTuningJobConfig() *sagemakerSpec.HyperparameterTuningJobConfig {
	return &sagemakerSpec.HyperparameterTuningJobConfig{
		HyperparameterRanges: &sagemakerSpec.ParameterRanges{ParameterRangeMap: generateMockTunableHPMap()},
		TuningStrategy:       sagemakerSpec.HyperparameterTuningStrategy_BAYESIAN,
		TuningObjective: &sagemakerSpec.HyperparameterTuningObjective{
			ObjectiveType: sagemakerSpec.HyperparameterTuningObjectiveType_MINIMIZE,
			MetricName:    "validate:mse",
		},
		TrainingJobEarlyStoppingType: sagemakerSpec.TrainingJobEarlyStoppingType_AUTO,
	}
}

func Test_deleteConflictingStaticHyperparameters(t *testing.T) {
	mockCtx := context.TODO()
	type args struct {
		ctx          context.Context
		staticHPs    []*commonv1.KeyValuePair
		tunableHPMap map[string]*sagemakerSpec.ParameterRangeOneOf
	}
	tests := []struct {
		name string
		args args
		want []*commonv1.KeyValuePair
	}{
		{name: "Partially conflicting hyperparameter list", args: args{
			ctx:          mockCtx,
			staticHPs:    generatePartiallyConflictingStaticHPs(),
			tunableHPMap: generateMockTunableHPMap(),
		}, want: []*commonv1.KeyValuePair{{Name: "hp4", Value: "0.5"}}},
		{name: "Totally conflicting hyperparameter list", args: args{
			ctx:          mockCtx,
			staticHPs:    generateTotallyConflictingStaticHPs(),
			tunableHPMap: generateMockTunableHPMap(),
		}, want: []*commonv1.KeyValuePair{}},
		{name: "Non-conflicting hyperparameter list", args: args{
			ctx:          mockCtx,
			staticHPs:    generateNonConflictingStaticHPs(),
			tunableHPMap: generateMockTunableHPMap(),
		}, want: []*commonv1.KeyValuePair{{Name: "hp5", Value: "100"}, {Name: "hp4", Value: "0.5"}, {Name: "hp7", Value: "ddd,eee"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deleteConflictingStaticHyperparameters(tt.args.ctx, tt.args.staticHPs, tt.args.tunableHPMap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deleteConflictingStaticHyperparameters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buildParameterRanges(t *testing.T) {
	type args struct {
		hpoJobConfig *sagemakerSpec.HyperparameterTuningJobConfig
	}
	tests := []struct {
		name string
		args args
		want *commonv1.ParameterRanges
	}{
		{name: "Building a list of a mixture of all three types of parameter ranges",
			args: args{hpoJobConfig: generateMockHyperparameterTuningJobConfig()},
			want: &commonv1.ParameterRanges{
				CategoricalParameterRanges: []commonv1.CategoricalParameterRange{{Name: ToStringPtr("hp3"), Values: []string{"AAA", "BBB", "CCC"}}},
				ContinuousParameterRanges:  []commonv1.ContinuousParameterRange{{Name: ToStringPtr("hp2"), MinValue: ToStringPtr("3.0"), MaxValue: ToStringPtr("5.0")}},
				IntegerParameterRanges:     []commonv1.IntegerParameterRange{{Name: ToStringPtr("hp1"), MinValue: ToStringPtr("0"), MaxValue: ToStringPtr("10")}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildParameterRanges(tt.args.hpoJobConfig)

			wantCatPr := tt.want.CategoricalParameterRanges[0]
			gotCatPr := got.CategoricalParameterRanges[0]
			if *wantCatPr.Name != *gotCatPr.Name || !reflect.DeepEqual(wantCatPr.Values, gotCatPr.Values) {
				t.Errorf("buildParameterRanges(): CategoricalParameterRange: got [Name: %v, Value: %v], want [Name: %v, Value: %v]",
					*gotCatPr.Name, gotCatPr.Values, *wantCatPr.Name, wantCatPr.Values)
			}
			wantIntPr := tt.want.IntegerParameterRanges[0]
			gotIntPr := got.IntegerParameterRanges[0]
			if *wantIntPr.Name != *gotIntPr.Name || *wantIntPr.MinValue != *gotIntPr.MinValue || *wantIntPr.MaxValue != *gotIntPr.MaxValue {
				t.Errorf("buildParameterRanges(): IntegerParameterRange: got [Name: %v, MinValue: %v, MaxValue: %v], want [Name: %v, MinValue: %v, MaxValue: %v]",
					*gotIntPr.Name, *gotIntPr.MinValue, *gotIntPr.MaxValue, *wantIntPr.Name, *wantIntPr.MinValue, *wantIntPr.MaxValue)
			}
			wantConPr := tt.want.ContinuousParameterRanges[0]
			gotConPr := got.ContinuousParameterRanges[0]
			wantMin, _ := strconv.ParseFloat(*wantConPr.MinValue, 64)
			gotMin, err := strconv.ParseFloat(*gotConPr.MinValue, 64)
			if err != nil {
				t.Errorf("buildParameterRanges(): ContinuousParameterRange: got invalid min value [%v]", gotMin)
			}
			wantMax, _ := strconv.ParseFloat(*wantConPr.MaxValue, 64)
			gotMax, err := strconv.ParseFloat(*gotConPr.MaxValue, 64)
			if err != nil {
				t.Errorf("buildParameterRanges(): ContinuousParameterRange: got invalid max value [%v]", gotMax)
			}
			if *wantConPr.Name != *gotConPr.Name || wantMin != gotMin || wantMax != gotMax {
				t.Errorf("buildParameterRanges(): ContinuousParameterRange: got [Name: %v, MinValue: %v, MaxValue: %v], want [Name: %v, MinValue: %v, MaxValue: %v]",
					*gotConPr.Name, gotMin, gotMax, *wantConPr.Name, wantMin, wantMax)
			}
		})
	}
}
