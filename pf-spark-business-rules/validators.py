from abc import ABCMeta, abstractmethod


def validate_rdd(rdd, validators):
    """"""

    print('validate_rdd')

    for x in xrange(0, len(validators)):
        validator = validators[x]
        rdd = rdd\
            .filter(lambda object: validator.lambda_function(object))
    return rdd


class Validator:
    """
    Validator is an Abstract Class
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def lambda_function(self):
        pass


class ValidatorType(object):
    """
    ValidatorType is a collection of constants.
    """

    num_equal = 'num_equal'
    num_less = 'num_less'
    num_greater = 'num_greater'
    num_not_equal = 'num_not_equal'
    num_not_less = 'num_not_less'
    num_not_greater = 'num_not_greater'
    datetime_match = 'datetime_match'
    datetime_before = 'datetime_before'
    datetime_after = 'datetime_after'
    string_match = 'string_match'


class ValidatorFactory(object):
    """ValidatorFactory"""

    kType = "type"
    kAttribute = "attribute"
    kParameters = "parameters"

    @staticmethod
    def from_json(json):
        type = json[ValidatorFactory.kType]
        attribute = json[ValidatorFactory.kAttribute]
        parameters = json[ValidatorFactory.kParameters]

        if type == ValidatorType.num_equal:
            return EqualToValidator(attribute, parameters[0])
        elif type == ValidatorType.num_less:
            return LessThanValidator(attribute, parameters[0])
        elif type == ValidatorType.num_greater:
            return GreaterThanValidator(attribute, parameters[0])
        elif type == ValidatorType.num_not_equal:
            return NotEqualToValidator(attribute, parameters[0])
        elif type == ValidatorType.num_not_less:
            return NotLessThanValidator(attribute, parameters[0])
        elif type == ValidatorType.num_not_greater:
            return NotGreaterThanValidator(attribute, parameters[0])
        elif type == ValidatorType.datetime_match:
            return MatchDateTimeComparison(attribute, parameters[0])
        elif type == ValidatorType.datetime_before:
            return BeforeDateTimeComparison(attribute, parameters[0])
        elif type == ValidatorType.datetime_after:
            return AfterDateTimeComparison(attribute, parameters[0])
        elif type == ValidatorType.string_match:
            return StringMatchComparison(attribute, parameters[0])
        return None


class Comparison(object):
    """
    The Comparison encapsulates logic of a comparison that can be translated
    into a Spark Transformation.

    Attributes:
    attribute_name (string):
    validator_type (ValidatorType):
    """
    def __init__(self, attribute_name, validator_type):
        super(Comparison, self).__init__()
        self.attribute_name = attribute_name
        self.validator_type = validator_type

    def __test_value__(self, test_object):
        if isinstance(test_object, dict):
            return test_object[self.attribute_name]
        elif isinstance(test_object, object):
            return getattr(test_object, self.attribute_name)
        return None


class NumericComparison(Comparison):
    """
    The NumericComparison encapsulates logic of a comparison between numerical
    objects.

    Attributes:
        number (float): numeric constant that the attribute shall
        be evaluated against
    """

    def __init__(self, attribute_name, validator_type, number):
        super(NumericComparison, self,).__init__(attribute_name,
                                                 validator_type)
        self.number = float(number)

    def __test_value__(self, test_object):
        if isinstance(test_object, dict):
            return test_object[self.attribute_name]
        elif isinstance(test_object, object):
            return getattr(test_object, self.attribute_name)
        return None


class EqualToValidator(NumericComparison, Validator):

    def __init__(self, attribute_name, number):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.num_equal,
                                              number)

    def lambda_function(self, test_object):
        test_value = float(self.__test_value__(test_object))
        return (test_value == self.number)


class LessThanValidator(NumericComparison, Validator):

    def __init__(self, attribute_name, number):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.num_less,
                                              number)

    def lambda_function(self, test_object):
        test_value = float(self.__test_value__(test_object))
        return (test_value < self.number)


class GreaterThanValidator(NumericComparison, Validator):

    def __init__(self, attribute_name, number):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.num_greater,
                                              number)

    def lambda_function(self, test_object):
        test_value = float(self.__test_value__(test_object))
        return (test_value > self.number)


class NotEqualToValidator(NumericComparison, Validator):

    def __init__(self, attribute_name, number):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.num_not_equal,
                                              number)

    def lambda_function(self, test_object):
        test_value = float(self.__test_value__(test_object))
        return (test_value != self.number)


class NotLessThanValidator(NumericComparison, Validator):

    def __init__(self, attribute_name, number):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.num_not_less,
                                              number)

    def lambda_function(self, test_object):
        test_value = float(self.__test_value__(test_object))
        return (test_value >= self.number)


class NotGreaterThanValidator(NumericComparison, Validator):

    def __init__(self, attribute_name, number):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.num_not_greater,
                                              number)

    def lambda_function(self, test_object):
        test_value = float(self.__test_value__(test_object))
        return (test_value <= self.number)


class DateTimeComparison(Comparison):
    """
    The DateTimeComparison encapsulates logic of a comparison between date,
    times, and datetime objects.

    Attributes:
        datetime (datetime): datetime constant that the attribute shall
        be evaluated against
    """
    def __init__(self, attribute_name, validator_type, date_time):
        super(DateTimeComparison, self,).__init__(attribute_name,
                                                  validator_type)
        self.datetime = date_time


class MatchDateTimeComparison(DateTimeComparison, Validator):

    def __init__(self, attribute_name, date_time):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.datetime_before,
                                              date_time)

    def lambda_function(self, test_object):
        # print('lambda_function')
        # print(test_object)

        test_value = self.__test_value__(test_object)
        return (test_value == self.datetime)


class BeforeDateTimeComparison(DateTimeComparison, Validator):

    def __init__(self, attribute_name, date_time):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.datetime_before,
                                              date_time)

    def lambda_function(self, test_object):
        test_value = self.__test_value__(test_object)
        return (test_value < self.datetime)


class AfterDateTimeComparison(DateTimeComparison, Validator):

    def __init__(self, attribute_name, date_time):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.datetime_after,
                                              date_time)

    def lambda_function(self, test_object):
        test_value = self.__test_value__(test_object)
        return (test_value > self.datetime)


class StringComparison(Comparison):
    """
    The StringComparison encapsulates logic of a comparison between strings.

    Attributes:
        string (string): string constant that the attribute shall
        be evaluated against
    """
    def __init__(self, attribute_name, validator_type, string):
        super(DateTimeComparison, self,).__init__(attribute_name,
                                                  validator_type)
        self.string = string


class StringMatchComparison(object):

    def __init__(self, attribute_name, string):
        super(self.__class__, self,).__init__(attribute_name,
                                              ValidatorType.string_match,
                                              string)
