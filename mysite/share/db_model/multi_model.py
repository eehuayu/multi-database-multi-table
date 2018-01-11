# -*- coding: utf-8 -*-
import ctypes
import sys

import binascii
from django.db import models
from django.db.models import F, IntegerField, CharField
from django.forms import model_to_dict

from share.utils import ObjectDict


class CustomBaseModel(models.Model):

    # 需要检验不能修改为非负数的字段列表,定义成常量,避免业务层更新时忘记填写
    NEG_CHECK_FIELDS = None

    class Meta:
        # 抽象类,不会在数据库里面生成实际的表结构
        abstract = True

    @classmethod
    def parse_delta_params(cls, params_dict):
        """
        解析差值更新参数,支持__x差值更新
        :param params_dict:
            {
                gold=1,         # gold = 1
                diamond__x=1,   # diamond += 1
                chips__x=-1,    # chips += -1
            }
        :return:
        """
        update_kwargs = dict()
        query_kwargs = dict()

        for p_k, p_v in params_dict.items():
            if "__x" in p_k:
                # 是否以差值更新
                as_delta = True
            else:
                as_delta = False

            real_k = p_k.replace("__x", "")

            if cls.NEG_CHECK_FIELDS and real_k in cls.NEG_CHECK_FIELDS and p_v < 0:
                # 更新负数,看看数据库里面的值是否大于等于要减去的数值,数据库原先的值不够大时,将查询不到对应的数据来修改
                query_kwargs[real_k + '__gte'] = abs(p_v)

            if as_delta:
                update_kwargs[real_k] = F(real_k) + p_v
            else:
                update_kwargs[real_k] = p_v

        return update_kwargs, query_kwargs

    @classmethod
    def get_all_fields(cls, *args, **kwargs):
        """
        返回model里面定义的所有字段名称
        :param args:
        :param kwargs:
        :return:
        """
        return cls._meta.get_all_field_names()

    @classmethod
    def model_to_dict_obj(cls, model):
        """
        model对象转字典对象,支持字典的点号读取字段
        :param model:
        :return:
        """
        return ObjectDict(**model_to_dict(model))

    @classmethod
    def convert_fields_type(cls, cache_values):
        """
        转换redis存储的类型为python类型
        :param cache_values:
        :return:
        """
        py_values = ObjectDict()

        fields_name_dict = cls.get_fields_dict()

        for key, value in cache_values.iteritems():
            py_values[key] = fields_name_dict[key].to_python(value)

        return py_values


class MultiTableModel(CustomBaseModel):

    class Meta:
        # 抽象类,不会在数据库里面生成实际的表结构
        abstract = True

    # 默认的分表数量
    table_count = 10

    # 分表字段名
    ROUTE_FIELD_NAME = None

    @classmethod
    def gen_cls(cls, idx):
        """
        根据idx取模生成应的model类
        :param idx:
        :return:
        """
        piece = cls.mod_table_index(idx)

        cls_name = cls.gen_cls_name(piece)

        class MetaNew(cls.Meta):
            abstract = False
            db_table = '%s%s' % (cls.Meta.db_table, idx)
            verbose_name = verbose_name_plural = u"%s(%s)" % (cls.Meta.verbose_name, cls_name)

        attrs = {
            '__module__': cls.__module__,
            'Meta': MetaNew,
        }

        return type(cls_name, (cls, ), attrs)

    @classmethod
    def route_func(cls, key_value):
        """
        根据指定分表字段的类型,对字段值转换为整数再取模计算出分表索引值
        :param key_value:
        :return:
        """
        field = cls._meta.get_field(cls.ROUTE_FIELD_NAME)

        if isinstance(field, IntegerField):
            int_value = int(key_value)

        elif isinstance(field, CharField):
            int_value = cls.uint_crc32(key_value)

        else:
            raise Exception('invalid field to split table, name: %s, value: %s' % cls.ROUTE_FIELD_NAME, key_value)

        return cls.mod_table_index(int_value)

    @classmethod
    def get_objects(cls, key_value):
        """
        获得对应idx的model的objects对象
        操作多表model的任何数据,都必须使用该函数
        :param idx:
        :return:
        """
        idx = cls.route_func(key_value)
        model = cls.gen_cls(idx)
        return getattr(model, 'objects')

    @classmethod
    def get_objects_by_idx(cls, idx):
        """
        只根据idx就拿到对应类的objects
        :param idx:
        :return:
        """
        model = cls.gen_cls(idx)
        return getattr(model, 'objects')

    @classmethod
    def filter(cls, key_value, *args, **kwargs):
        """
        封装objects.filter,自动查找对应的表
        :param key_value: 分表字段的值
        :param kwargs:
        :return:
        """
        assert key_value is not None

        kwargs.update({
            cls.ROUTE_FIELD_NAME: key_value,
        })

        return cls.get_objects(key_value).filter(*args, **kwargs)

    @classmethod
    def filter_x(cls, key_value, *args, **kwargs):
        """
        封装objects.filter,自动查找对应的表
        :param key_value: 分表字段的值, 不用查询,只用来查找对应的表
        :param kwargs:
        :return:
        """
        assert key_value is not None

        return cls.get_objects(key_value).filter(*args, **kwargs)

    @classmethod
    def gen_cls_name(cls, idx):
        """
        生成类名后缀
        :param idx:
        :return:
        """
        return cls.__name__ + str(idx)

    @classmethod
    def mod_table_index(cls, idx):
        """
        用整数取模表个数分表
        :param idx:
        :return:
        """
        return int(idx) % cls.table_count

    @classmethod
    def uint_crc32(cls, s):
        """
        把一个字符串经过CRC32哈希后,再转换为unsigned int
        :param s:
        :return:
        """
        return ctypes.c_uint32(binascii.crc32(str(s))).value

    @classmethod
    def mod_table_index_crc32(cls, s):
        """
        先转整数再取模
        :param s:
        :return:
        """
        return cls.mod_table_index(cls.uint_crc32(s))

    @classmethod
    def get_model_by_crc32(cls, s):
        """
        用传入参数哈希后转换为无符号整数再取模找到对应的表
        :param s: 字符串
        :return:
        """
        return cls.gen_cls(cls.mod_table_index_crc32(s))

    @classmethod
    def get_all_fields(cls, idx):
        """
        返回model里面定义的所有字段名称
        :param idx:
        :return:
        """
        cls_obj = cls.gen_cls(idx)
        return cls_obj._meta.get_all_field_names()

    @classmethod
    def get_fields_dict(cls):
        """
        获取model所定义的字段以name作为key的字典
        :return:
        """
        return {field.name: field for field in cls._meta.fields}

    @classmethod
    def create(cls, key_value, **kwargs):
        """
        create时会查找对应的表
        :param key_value: 分表的字段
        :param kwargs:
        :return:
        """
        assert key_value is not None

        kwargs.update({
            cls.ROUTE_FIELD_NAME: key_value,
        })

        return cls.get_objects(key_value).create(**kwargs)

    @classmethod
    def update(cls, key_value, **kwargs):
        """
        update时会查找对应的表
        :param key_value: 分表的字段
        :param kwargs:
        :return:
        """
        assert key_value is not None

        kwargs.update({
            cls.ROUTE_FIELD_NAME: key_value,
        })

        cls.get_objects(key_value).update(**kwargs)

    @classmethod
    def filter_update(cls, key_value, filter_kwargs, update_kwargs):
        """
        方便更新缓存
        :param key_value: 分表的字段
        :param filter_kwargs:
        :param update_kwargs:
        :return:
        """
        assert key_value is not None

        filter_kwargs.update({
            cls.ROUTE_FIELD_NAME: key_value,
        })

        return cls.get_objects(key_value).filter(**filter_kwargs).update(**update_kwargs)

    @classmethod
    def update_or_create(cls, key_value, filter_kwargs, update_kwargs):
        """
        更新或者创建对象,如果查询的数据已经存在,就更新
        :param key_value:
        :param filter_kwargs:
        :param update_kwargs:
        :return:
        """
        idx = cls.route_func(key_value)
        model = cls.gen_cls(idx)

        fields = dict()
        fields.update(filter_kwargs)
        fields.update(update_kwargs)

        try:
            obj = model.objects.get(**filter_kwargs)
            for key, value in update_kwargs.iteritems():
                setattr(obj, key, value)
            obj.save()
        except model.DoesNotExist:
            obj = model(**fields)
            obj.save()

        return True


def gen_multi_model(cls):
    """
    自动分表,自动在cls所在的module里面生成cls.table_count个model
    :param cls:
    :return:
    """
    module = sys.modules[cls.__module__]

    for idx in range(cls.table_count):
        cls_obj = cls.gen_cls(idx)
        setattr(module, cls_obj.__name__, cls_obj)

    return cls
