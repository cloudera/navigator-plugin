/*
 * Copyright (c) 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.nav.sdk.client.writer.registry;

import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.relations.Relation;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.cloudera.nav.sdk.model.relations.RelationType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

/**
 * Represents a single relationship declared in a Entity's MRelation annotated
 * getter method
 */
public class MRelationEntry {

  private final Field field;
  private final Method getter;
  private final MRelation relationAnn;
  private final Map<RelationRole, RelationType> roleToTypeMap;

  public MRelationEntry(Field field, Method getter) {
    this.field = field;
    this.getter = getter;
    this.relationAnn = field.getAnnotation(MRelation.class);
    roleToTypeMap = Maps.newHashMap();
    validateReturnType(getter);
  }

  /**
   * Build Relation based on the entity and the MRelation annotation information
   * Note that this MRelationEntry's associated method must exist in the
   * entity's class
   *
   * @param entity
   * @return
   */
  public Relation buildRelation(Entity entity, String namespace) {
    RelationRole roleOfOther = relationAnn.role();
    RelationType type = getRelationTypeFromRole(roleOfOther);
    RelationFactory factory = new RelationFactory();
    Collection<? extends Entity> otherEndpoints = getConnectedEntities(entity);

    return factory.createRelation(type, entity, otherEndpoints, roleOfOther,
        namespace);
  }

  private RelationType getRelationTypeFromRole(RelationRole roleOfOther) {
    if (roleToTypeMap.size() == 0) {
      for (RelationType type : RelationType.values()) {
        roleToTypeMap.put(type.getEndpoint1Role(), type);
        roleToTypeMap.put(type.getEndpoint2Role(), type);
      }
    }
    return roleToTypeMap.get(roleOfOther);
  }

  /**
   * @return collection of entities connected to the given entity through the
   * relationship represented by this MRelationEntry
   */
  @SuppressWarnings("unchecked")
  public Collection<? extends Entity> getConnectedEntities(Entity entity) {
    // we've already validated the return type in the c'tor
    if (Entity.class.isAssignableFrom(field.getType())) {
      return ImmutableList.of((Entity)getValue(entity));
    } else {
      return (Collection<Entity>)getValue(entity);
    }
  }

  private Object getValue(Entity entity) {
    try {
      return getter.invoke(entity);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  private void validateReturnType(Method method) {
    // String, Entity, Collection<String>, Collection<Entity>
    Class<?> aClass = method.getReturnType();
    Preconditions.checkArgument(Entity.class.isAssignableFrom(aClass) ||
          (Collection.class.isAssignableFrom(aClass) &&
              Entity.class.isAssignableFrom(getTypeParameterClass())),
        "@MRelation fields must be an Entity or a Collection of Entities");
  }

  private Class<?> getTypeParameterClass() {
    ParameterizedType type = (ParameterizedType) field.getGenericType();
    Type[] typeParams = type.getActualTypeArguments();
    Preconditions.checkArgument(typeParams.length == 1);
    return (Class<?>)typeParams[0];
  }

  /**
   * @return whether this @MRelation field is required
   */
  public boolean required() {
    return relationAnn.required();
  }

  /**
   * @return the name of the field associated with this @MRelation entry
   */
  public String getName() {
    return field.getName();
  }
}
