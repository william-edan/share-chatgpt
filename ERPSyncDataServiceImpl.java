package com.vevor.mdm.goods.service.sync.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.vevor.center.user.vo.GroupVO;
import com.vevor.mdm.goods.constant.DataStatusConstants;
import com.vevor.mdm.goods.constant.DictConstant;
import com.vevor.mdm.goods.constant.ERPConstant;
import com.vevor.mdm.goods.constant.ErpConstants;
import com.vevor.mdm.goods.convert.DataSynchronizedConvert;
import com.vevor.mdm.goods.dao.dataobject.*;
import com.vevor.mdm.goods.dao.dataobject.SkuRelationDO;
import com.vevor.mdm.goods.dao.service.*;
import com.vevor.mdm.goods.entity.attribute.GoodsAttributeValueDO;
import com.vevor.mdm.goods.entity.goods.*;
import com.vevor.mdm.goods.entity.goods.SkuDO;
import com.vevor.mdm.goods.enums.SkuRelationTypeEnum;
import com.vevor.mdm.goods.enums.SkuTypeEnum;
import com.vevor.mdm.goods.model.erp.*;
import com.vevor.mdm.goods.php.PhpClientConnection;
import com.vevor.mdm.goods.php.PhpData;
import com.vevor.mdm.goods.service.biz.IGoodsSizeInfoService;
import com.vevor.mdm.goods.service.biz.ISkuCustomsDeclaredProductInfoService;
import com.vevor.mdm.goods.service.biz.ISkuPurchaseInfoService;
import com.vevor.mdm.goods.service.biz.ISkuSupplierInfoService;
import com.vevor.mdm.goods.service.biz.impl.SkuService;
import com.vevor.mdm.goods.service.sync.IERPSyncDataService;
import com.vevor.mdm.goods.service.rpc.RpcClientService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @version :  V1.0
 * @prgram :  IntelliJ IDEA
 * @Author :  edan
 * @date :  created in 2023/4/3
 * @description :
 */
@Service
@Slf4j
public class ERPSyncDataServiceImpl implements IERPSyncDataService {

    @Resource
    private PhpClientConnection phpClientConnection;
    @Resource
    private SkuService skuService;
    @Resource
    private RpcClientService rpcClientService;
    @Resource
    private MgsSpuService mgsSpuService;
    @Resource
    private MgsConfDictionaryService dictionaryService;
    @Resource
    private MgsSkuRelationService mgsSkuRelationService;
    @Resource
    private MgsAttributeItemService attributeItemService;
    @Resource
    private MgsAttributeValueService attributeValueService;
    @Resource
    private MgsSkuImageService skuImageService;
    @Resource
    private ISkuCustomsDeclaredProductInfoService skuCustomsDeclaredProductInfoService;
    @Resource
    private IGoodsSizeInfoService goodsSizeInfoService;
    @Resource
    private MgsSkuAttributeValueService skuAttributeValueService;
    @Resource
    private ISkuSupplierInfoService skuSupplierInfoService;
    @Resource
    private ISkuPurchaseInfoService skuPurchaseInfoService;

    @Override
    public void syncSkuBasicInfo(List<SkuDO> dataList) {
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        List<SyncBasicInfoToErpModel> arrayList = dataList.stream()
                .filter(skuDO -> skuDO.getMainSkuId() != 0L)
                .filter((sku)->isSync(sku.getSkuId()))
                .map(this::getBasicInfo)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(arrayList)) {
            todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL, ERPConstant.TO_ERP_MODULE,
                    ERPConstant.BASICINFO_TO_ERP_ACTION, "货品质料", arrayList));
        }
    }

    @Override
    public void syncSkuSizeInfo(List<GoodsSizeInfo> dataList) {
        List<Map<String, Object>> sizeList = new ArrayList<>();
        List<SyncBasicInfoToErpModel> basicInfoToErpModels = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(dataList)) {
            List<SkuDO> skuDOS = skuService.listByIds(dataList.stream().map(GoodsSizeInfo::getSkuId).collect(Collectors.toSet()));
            Map<Long, SkuDO> groupBySKuId = skuDOS.stream().collect(Collectors.toMap(SkuDO::getSkuId, Function.identity()));
            for (GoodsSizeInfo goodsSizeInfo : dataList) {
                Map<String, Object> dataMap = new HashMap<String, Object>();
                SkuDO skuDO = groupBySKuId.get(goodsSizeInfo.getSkuId());
                if (null == skuDO) {
                    rpcClientService.sendDTalkMessage("尺寸信息同步至Erp失败 sku不存在:"+skuDO.getSkuId());
                    continue;
                }
                if (isSync(skuDO.getSkuId())) {
                    //erp那边的skuCode是我们这边的带电压skuCode
                    dataMap.put("skuCode", skuDO.getSkuCode());
                    dataMap.put("lengthCm", goodsSizeInfo.getPackingLengthCm());
                    dataMap.put("widthCm", goodsSizeInfo.getPackingWidthCm());
                    dataMap.put("heightCm", goodsSizeInfo.getPackingHeightCm());
                    dataMap.put("weightKg", goodsSizeInfo.getPackingWeightKg());
                    sizeList.add(dataMap);
                    if (SkuTypeEnum.PART.type.equals(skuDO.getType())) {
                        SyncBasicInfoToErpModel basicInfo = getBasicInfo(skuDO);
                        basicInfoToErpModels.add(basicInfo);
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(sizeList)) {
                todoSync(new PHPSyncRequestModel( ERPConstant.TO_ERP_URL, ERPConstant.TO_ERP_MODULE,ERPConstant.SIZEINFO_TO_ERP_ACTION,
                        "尺寸信息",sizeList));
            }
            if (CollectionUtils.isNotEmpty(basicInfoToErpModels)) {
                todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.TO_ERP_MODULE,ERPConstant.BASICINFO_TO_ERP_ACTION,
                        "尺寸信息", basicInfoToErpModels
                ));
            }
        }

    }

    @Override
    public void syncSkuSupplier(List<SkuSupplierInfoDO> skuSupplierInfoDOS) {
        if (CollectionUtils.isNotEmpty(skuSupplierInfoDOS)) {
            Set<Long> skuIds = skuSupplierInfoDOS.stream().map(SkuSupplierInfoDO::getSkuId).collect(Collectors.toSet());
            List<SkuDO> skuDOS = skuService.listByIds(skuIds);
            if (CollectionUtils.isEmpty(skuDOS)) {
                rpcClientService.sendDTalkMessage(StrUtil.format("同步供应商失败 sku不存在{}",skuIds));
            }
            List<SupplierInfoVO> syncErpSupplierList = skuDOS.stream().filter((sku)->isSync(sku.getSkuId())).map(skuDO -> {
               return getSyncErpSupplierInfo(skuDO);
            }).filter(Objects::nonNull).collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(syncErpSupplierList)) {
                todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.TO_ERP_MODULE,ERPConstant.BATCH_SUPPLIER_INFO_RM_ATION, "供应商", syncErpSupplierList
                ));
            }
        }
    }

    @Override
    public void syncPurchaseInfoToErp(List<SkuPurchaseInfoDO> skuPurchaseInfoDOS) {
        if (CollectionUtils.isNotEmpty(skuPurchaseInfoDOS)) {
            log.info("推送采购信息至erp {}",JSON.toJSONString(skuPurchaseInfoDOS));
            List<SkuPurchaseInfoDO> needSyncToErpList = skuPurchaseInfoDOS.stream().filter((skuDo -> this.isSync(skuDo.getSkuId()))).collect(Collectors.toList());
            Set<Long> skuIds = needSyncToErpList.stream().map(SkuPurchaseInfoDO::getSkuId).collect(Collectors.toSet());
            List<SkuDO> list = skuService.lambdaQuery().in(SkuDO::getSkuId, skuIds).eq(SkuDO::getStatus, DataStatusConstants.SKU_STATUS_TAKE_EFFECT).list();
            if (CollectionUtils.isEmpty(list)) {
                return;
            }
            Map<Long, SkuDO> skuMap = list.stream().collect(Collectors.toMap(SkuDO::getSkuId,Function.identity()));
            if (CollectionUtils.isNotEmpty(needSyncToErpList)) {
                List<ErpLowestPurchaseNumberVO> erpLowestPurchaseNumberVOS = new ArrayList<>();
                List<SupplierInfoVO> syncErpSupplierList = new ArrayList<>();
                needSyncToErpList.stream().forEach(skuPurchaseInfoDO -> {
                    SkuDO skuDO = skuMap.get(skuPurchaseInfoDO.getSkuId());
                    if (null == skuDO) {
                        rpcClientService.sendDTalkMessage("推送采购信息至ERP失败 sku不存在 :"+skuPurchaseInfoDO.getSkuId());
                        return;
                    }
                    syncErpSupplierList.add(getSyncErpSupplierInfo(skuDO));
                    erpLowestPurchaseNumberVOS.add(new ErpLowestPurchaseNumberVO(skuDO.getSkuCode(), skuPurchaseInfoDO.getMinimumOrderQuantity()));
                });
                if (CollectionUtils.isNotEmpty(erpLowestPurchaseNumberVOS)) {
                    todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.TO_ERP_MODULE,ERPConstant.PHP_PURCHASE_LOWEST_NUM_ACTION, "采购信息", erpLowestPurchaseNumberVOS
                    ));
                }
                if (CollectionUtils.isNotEmpty(syncErpSupplierList)) {
                    todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.TO_ERP_MODULE,ERPConstant.BATCH_SUPPLIER_INFO_RM_ATION, "采购信息", syncErpSupplierList
                    ));
                }
            };


        }
    }

    @Override
    public void syncCustomsDeclared(List<SkuCustomsDeclaredProductInfoDO> customsDeclaredProductInfoDOS) {
        if (CollectionUtils.isEmpty(customsDeclaredProductInfoDOS)) {
            return;
        }
        log.info("推送报关信息至erp {}",JSON.toJSONString(customsDeclaredProductInfoDOS));
        //获取需要同步的数据
        List<SkuCustomsDeclaredProductInfoDO> needSyncToErpList = customsDeclaredProductInfoDOS.stream().filter((skuDo -> this.isSync(skuDo.getSkuId()))).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(needSyncToErpList)) {
            return;
        }
        Set<Long> skuIds = needSyncToErpList.stream().map(SkuCustomsDeclaredProductInfoDO::getSkuId).collect(Collectors.toSet());

        // 查询相应的 SkuDO 记录，并构建 skuMap 映射
        Map<Long, SkuDO> skuMap = skuService.lambdaQuery()
                .in(SkuDO::getSkuId, skuIds)
                .eq(SkuDO::getStatus, DataStatusConstants.SKU_STATUS_TAKE_EFFECT)
                .list()
                .stream()
                .collect(Collectors.toMap(SkuDO::getSkuId, Function.identity()));
        List<SupplierInfoVO> syncErpSupplierList = new ArrayList<>();
        List<SyncBasicInfoToErpModel> syncErpBasicInfo = new ArrayList<>();
        // 遍历需要同步的记录，并构建相应的数据
        needSyncToErpList.stream().forEach(customsDeclaredProductInfoDO->{
            SkuDO skuDO = skuMap.get(customsDeclaredProductInfoDO.getSkuId());
            if (null == skuDO) {
                rpcClientService.sendDTalkMessage("推送报关信息至ERP失败 sku不存在 :"+customsDeclaredProductInfoDO.getSkuId());
                return;
            }
            syncErpBasicInfo.add(getBasicInfo(skuDO));
            syncErpSupplierList.add(getSyncErpSupplierInfo(skuDO));
        });
        if (CollectionUtils.isNotEmpty(syncErpSupplierList)) {
            todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.TO_ERP_MODULE,ERPConstant.BATCH_SUPPLIER_INFO_RM_ATION,
                    "报关信息", syncErpSupplierList
            ));
        }
        if (CollectionUtils.isNotEmpty(syncErpBasicInfo)) {
            todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.TO_ERP_MODULE,ERPConstant.BASICINFO_TO_ERP_ACTION,
                    "报关信息", syncErpBasicInfo
            ));
        }

    }

    @Override
    public void syncSkuTakeEffectInfo(SyncSkuTakeEffectInfoModel skuTakeEffectInfoModel) {
        log.info("推送生效时间至erp {}",JSON.toJSONString(skuTakeEffectInfoModel));
        todoSync(new PHPSyncRequestModel(ERPConstant.TO_ERP_URL,ERPConstant.PHP_PARTNER_ORDER_DAY_UPDATE_MODULE,ERPConstant.PHP_PARTNER_ORDER_DAY_UPDATE_ACTION,
                "状态生效时间", skuTakeEffectInfoModel
        ));
    }

    @Override
    public void syncGoodsPackagingMaterial(List<GoodsAttributeValueDO> goodsAttributeValueDOS) {

        log.info("推送包装材质信息至erp {}",JSON.toJSONString(goodsAttributeValueDOS));
        Set<Long> skuIds = goodsAttributeValueDOS.stream().map(GoodsAttributeValueDO::getTargetId).collect(Collectors.toSet());
        List<SyncBasicInfoToErpModel> basicInfoToErpModels = skuIds.stream().map(item -> {
            SkuDO skuDO = skuService.getById(item);
            if (null == skuDO) {
                rpcClientService.sendDTalkMessage(StrUtil.format("同步商品属性失败 sku不存在 {}", item));
                return null;
            }
            return getBasicInfo(skuDO);
        }).filter(Objects::nonNull).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(basicInfoToErpModels)) {
            PHPSyncRequestModel requestModel = new PHPSyncRequestModel(ERPConstant.TO_ERP_URL, ERPConstant.TO_ERP_MODULE,
                    ERPConstant.BASICINFO_TO_ERP_ACTION, "货品质料", basicInfoToErpModels);
            todoSync(requestModel);
        }
    }

    @Override
    public void syncUnPackInfo(List<com.vevor.mdm.goods.entity.goods.SkuRelationDO> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        log.info("推送拆包信息至erp {}",JSON.toJSONString(list));

        List<Map<String, Object>> syncErpUnpackList = new ArrayList<>();
        for (com.vevor.mdm.goods.entity.goods.SkuRelationDO skuRelationDO : list) {
            Long originalSkuId = skuRelationDO.getSkuId();
            if (!DataStatusConstants.SKU_STATUS_TAKE_EFFECT.equals(skuRelationDO.getStatus())) {
                return;
            }
            List<SkuRelationDO> unpackList = mgsSkuRelationService.lambdaQuery().eq(SkuRelationDO::getSkuId, originalSkuId).eq(SkuRelationDO::getType, SkuRelationTypeEnum.UNPACKING_COMBINATION.type).list();
            if (CollectionUtils.isEmpty(unpackList)) {
                return;
            }
            Set<Long> unpackSkuIdList = unpackList.stream().map(SkuRelationDO::getTargetSkuId).collect(Collectors.toSet());
            unpackSkuIdList.add(originalSkuId);
            List<SkuDO> takeEffectSkuList = skuService.lambdaQuery().eq(SkuDO::getSkuId, unpackSkuIdList).eq(SkuDO::getStatus, DataStatusConstants.SKU_STATUS_TAKE_EFFECT).list();
            if (takeEffectSkuList.size() != unpackSkuIdList.size()) {
                log.info("拆包sku不全部都是生效状态 {}",originalSkuId);
                return;
            }
            Map<String, Object> requestMap = new HashMap<>();
            List<String> unpackSubSkuCodeList = new ArrayList<>();
            Map<Long, String> skuCodeMap = takeEffectSkuList.stream().collect(Collectors.toMap(SkuDO::getSkuId, SkuDO::getSkuCode));
            for (SkuRelationDO relationDO : unpackList) {
                String features = relationDO.getFeatures();
                Map map = JSON.parseObject(features, Map.class);
                Object skuLevel = map.get(DictConstant.SKU_RELATION_FEATURE_SKU_LEVEL);
                String skuCode = skuCodeMap.get(relationDO.getTargetSkuId());
                //主sku
                if (null != skuLevel && DataStatusConstants.SKU_RELATION_FEATURE_SKU_MAIN_LEVEL.equals(Integer.parseInt(skuLevel.toString()))) {
                    requestMap.put(ErpConstants.UNPACK_MAIN_SKU, skuCodeMap.get(skuCode));
                }else {
                    unpackSubSkuCodeList.add(skuCode);
                }
            }
            requestMap.put(ErpConstants.UNPACK_SUB_SKU, unpackSubSkuCodeList);
            syncErpUnpackList.add(requestMap);
        }
        if (CollectionUtils.isNotEmpty(syncErpUnpackList)) {
            PHPSyncRequestModel requestModel = new PHPSyncRequestModel(ERPConstant.TO_ERP_URL, ERPConstant.TO_ERP_MODULE,
                    ERPConstant.PHP_SPLIT_PACKAGE_ACTION, "拆包商品", syncErpUnpackList);
            todoSync(requestModel);
        }
    }

    @Override
    public void syncSkuImage(List<GoodsProductImage> goodsProductImages) {
        if (CollectionUtils.isEmpty(goodsProductImages)) {
            return;
        }
        log.info("同步图片信息至erp {}",JSON.toJSONString(goodsProductImages));
        Set<Long> skuIds = goodsProductImages.stream().map(GoodsProductImage::getSkuId).collect(Collectors.toSet());
        List<SkuDO> list = skuService.lambdaQuery().in(SkuDO::getSkuId, skuIds).list();
        if (CollectionUtils.isEmpty(list)) {
            rpcClientService.sendDTalkMessage(StrUtil.format("同步图片失败 sku 不存在{}",skuIds.toString()));
            return;
        }
        Map<Long, SkuDO> skuDOMap = list.stream().collect(Collectors.toMap(SkuDO::getSkuId, Function.identity()));
        List<SyncBasicInfoToErpModel> syncBasicInfoToErpModelList =skuIds.stream().map(skuId -> {
            SkuDO skuDO = skuDOMap.get(skuId);
            if (null == skuDO) {
                rpcClientService.sendDTalkMessage(StrUtil.format("sku不存在，同步图片至erp失败 {}", skuId));
                return null;
            }
            return getBasicInfo(skuDO);
        }).filter(Objects::nonNull).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(syncBasicInfoToErpModelList)) {
            PHPSyncRequestModel requestModel = new PHPSyncRequestModel(ERPConstant.TO_ERP_URL, ERPConstant.TO_ERP_MODULE,
                    ERPConstant.BASICINFO_TO_ERP_ACTION, "货品质料-图片", syncBasicInfoToErpModelList);
            todoSync(requestModel);
        }
    }


    /**
     * 发起同步请求
     * @param phpSyncRequestModel
     */
    private void todoSync(PHPSyncRequestModel phpSyncRequestModel) {
        Map<String, String> requestMap = new HashMap<String, String>();
        requestMap.put("url", phpSyncRequestModel.getUrl());
        requestMap.put("module", phpSyncRequestModel.getModule());
        requestMap.put("action", phpSyncRequestModel.getAction());
        requestMap.put("json", JSONObject.toJSONString(phpSyncRequestModel.getData()));
        log.info("同步 {}到erp:{}", phpSyncRequestModel.getModuleStr(),JSONObject.toJSONString(phpSyncRequestModel.getData()));
        try {
            PhpData phpData = phpClientConnection.getDataModel(requestMap);
            if (phpData == null || !"200".equals(phpData.getCode())) {
                rpcClientService.sendDTalkMessage(String.format("同步至ERP失败:%s message:%s",phpSyncRequestModel.toString(),phpData.toString()));
            }
        } catch (Exception e) {
            rpcClientService.sendDTalkMessage(String.format("同步至ERP失败:%s message:%s",phpSyncRequestModel.toString(),e.getMessage()));
        }
    }


    private boolean isSync(Long skuId) {
        List<SkuRelationDO> list = mgsSkuRelationService.lambdaQuery().eq(SkuRelationDO::getSkuId, skuId).list();
        if (CollectionUtils.isNotEmpty(list)) {
            Integer type = list.stream().findFirst().get().getType();
            return !(SkuRelationTypeEnum.UNPACKING_COMBINATION.type.equals(type)
                    || SkuRelationTypeEnum.MARKETING_COMBINATION.type.equals(type));


        }
        return true;

    }



    private SupplierInfoVO getSyncErpSupplierInfo(SkuDO skuDO) {
        SkuSupplierInfoDO supplierInfoDO = skuSupplierInfoService.lambdaQuery().eq(SkuSupplierInfoDO::getSkuId, skuDO.getSkuId()).one();
        if (null == supplierInfoDO) {
            return null;
        }
        SupplierInfoVO supplierInfoVO = new SupplierInfoVO();
        supplierInfoVO.setSkuCode(skuDO.getSkuCode());
        supplierInfoVO.setNewPartner(supplierInfoDO.getSupplierName());
        if (DataStatusConstants.SKU_STATUS_TAKE_EFFECT.equals(skuDO.getStatus())) {
            addPurchaseSupplierInfo(skuDO, supplierInfoVO);
            addCustomsDeclaredSupplierInfo(skuDO, supplierInfoVO);
        }

        return supplierInfoVO;
    }

    private SupplierInfoVO addPurchaseSupplierInfo(SkuDO skuDO,SupplierInfoVO supplierInfoVO) {
        SkuPurchaseInfoDO skuPurchaseInfoDO = skuPurchaseInfoService.lambdaQuery().eq(SkuPurchaseInfoDO::getSkuId, skuDO.getSkuId()).one();
        if (null == skuPurchaseInfoDO) {
            return supplierInfoVO;
        }
        if ( skuPurchaseInfoDO.getInvoicingTaxRate() != null ) {
            supplierInfoVO.setInvoiceTax( new BigDecimal( skuPurchaseInfoDO.getInvoicingTaxRate() ) );
        }
        if ( skuPurchaseInfoDO.getTaxRefundRate() != null ) {
            supplierInfoVO.setDrawbackRate( new BigDecimal( skuPurchaseInfoDO.getTaxRefundRate() ) );
        }
        if ( skuPurchaseInfoDO.getInvoicingTaxRate() != null ) {
            supplierInfoVO.setInvoiceDot( new BigDecimal( skuPurchaseInfoDO.getInvoicingTaxRate() ) );
        }
        supplierInfoVO.setIsTaxRefund( skuPurchaseInfoDO.getIsIncludeTax()?1 :0 );
        ConfDictionaryDO dictionaryDO = dictionaryService.getById(skuPurchaseInfoDO.getDeliveryCycle());
        if (null != dictionaryDO) {
            supplierInfoVO.setDeliveryCycle(Integer.parseInt(dictionaryDO.getDataName()));


        }else {
            rpcClientService.sendDTalkMessage(StrUtil.format("交货周期字典不存在 {}",skuPurchaseInfoDO.getDeliveryCycle()));
        }
        return supplierInfoVO;
    }

    private SupplierInfoVO  addCustomsDeclaredSupplierInfo(SkuDO skuDO,SupplierInfoVO supplierInfoVO) {
        SkuCustomsDeclaredProductInfoDO customsDeclaredProductInfoDO = skuCustomsDeclaredProductInfoService.lambdaQuery()
                .eq(SkuCustomsDeclaredProductInfoDO::getSkuId, skuDO.getSkuId()).one();
        if (null == customsDeclaredProductInfoDO) {
            return supplierInfoVO;
        }
        ConfDictionaryDO unitDict = dictionaryService.getById(customsDeclaredProductInfoDO.getUnit());
        supplierInfoVO.setProductInvoiceName(customsDeclaredProductInfoDO.getInvoiceName());
        supplierInfoVO.setSkuCode(skuDO.getSkuCode());
        if (null != unitDict) {
            supplierInfoVO.setComputingUnit(unitDict.getDataName());
        }else {
            rpcClientService.sendDTalkMessage("推送报关信息至ERP失败 单位不存在 unit:"+customsDeclaredProductInfoDO.getUnit()
                    +"skuId:"+customsDeclaredProductInfoDO.getSkuId());
        }
        return supplierInfoVO;

    }


    private SyncBasicInfoToErpModel getBasicInfo(SkuDO skuDO) {
        String mainSkuCode = Optional.ofNullable(skuService.getById(skuDO.getMainSkuId())).map(SkuDO::getSkuCode).orElse(null);
        if (StringUtils.isBlank(mainSkuCode)) {
            rpcClientService.sendDTalkMessage("同步至Erp失败 主sku不存在:" + skuDO.getMainSkuId());
            return null;
        }
        SyncBasicInfoToErpModel syncBasicInfoToErpModel = new SyncBasicInfoToErpModel();
        syncBasicInfoToErpModel.setSkuCode(skuDO.getSkuCode());
        syncBasicInfoToErpModel.setNameCn(skuDO.getProductName());
        syncBasicInfoToErpModel.setCategory(SkuTypeEnum.getByType(skuDO.getType()).typeStr);
        syncBasicInfoToErpModel.setNameEn(skuDO.getProductName());
        syncBasicInfoToErpModel.setBrandCn(skuDO.getBrand());
        syncBasicInfoToErpModel.setBrandEn(skuDO.getBrand());
        syncBasicInfoToErpModel.setSkuWithplugCode(mainSkuCode);
        syncBasicInfoToErpModel.setIsSecondSale(SkuTypeEnum.SECOND_SALE.type.equals(skuDO.getType())?DataStatusConstants.YES:DataStatusConstants.NO);
        Integer unit = skuDO.getUnit();
        if (null != unit) {
            ConfDictionaryDO dictionaryDO = dictionaryService.getById(unit);
            if (null != dictionaryDO) {
                syncBasicInfoToErpModel.setUnit(dictionaryDO.getDataName());
            }
        }
        syncBasicInfoToErpModel.setSkuId(skuDO.getSkuId());
        syncBasicInfoToErpModel.setSpuId(skuDO.getSpuId());
        addGroupInfo(syncBasicInfoToErpModel);
        addDeclaredValue(skuDO,syncBasicInfoToErpModel);
        addGoodsImages(syncBasicInfoToErpModel);
        if (DataStatusConstants.SKU_STATUS_TAKE_EFFECT.equals(skuDO.getStatus())) {
            addCustomsDeclaredInfo(syncBasicInfoToErpModel);
            addPackagingMaterial(syncBasicInfoToErpModel);
        }
        return syncBasicInfoToErpModel;
    }

    private SyncBasicInfoToErpModel addGroupInfo(SyncBasicInfoToErpModel basicInfo) {
        SpuDO spuDO = Optional.ofNullable( mgsSpuService.getById(basicInfo.getSpuId())).orElse(null);
        if (spuDO == null) {
            return basicInfo;
        }
        Optional.ofNullable(spuDO.getGroupId()).ifPresent(groupId->{
            GroupVO groupVO = rpcClientService.getGroup(Collections.singleton(groupId)).get(groupId);
            if (groupVO != null) {
                basicInfo.setDeveloperGroup(groupVO.getGroupNumber());
            }
        });
        return basicInfo;
    }

    /**
     * 添加报关属性
     * @param syncBasicInfoToErpModel
     * @return
     */
    private SyncBasicInfoToErpModel addCustomsDeclaredInfo( SyncBasicInfoToErpModel syncBasicInfoToErpModel) {

        SkuCustomsDeclaredProductInfoDO customsDeclaredProductInfoDO = skuCustomsDeclaredProductInfoService.lambdaQuery()
                .eq(SkuCustomsDeclaredProductInfoDO::getSkuId, syncBasicInfoToErpModel.getSkuId()).one();
        if (null == customsDeclaredProductInfoDO) {
            return syncBasicInfoToErpModel;
        }
        syncBasicInfoToErpModel.setTypeCn(customsDeclaredProductInfoDO.getModel());
        syncBasicInfoToErpModel.setTypeEn(customsDeclaredProductInfoDO.getModel());
        syncBasicInfoToErpModel.setMaterialCn(customsDeclaredProductInfoDO.getMaterial());
        syncBasicInfoToErpModel.setMaterialEn(customsDeclaredProductInfoDO.getMaterial());
        syncBasicInfoToErpModel.setBrandEn(customsDeclaredProductInfoDO.getBrand());
        syncBasicInfoToErpModel.setBrandCn(customsDeclaredProductInfoDO.getBrand());
        syncBasicInfoToErpModel.setColor(customsDeclaredProductInfoDO.getColor());
        String productFeatures = customsDeclaredProductInfoDO.getProductFeatures();
        if (productFeatures != null && StringUtils.isNotEmpty(productFeatures)) {
            List<Integer> featuresList = JSON.parseArray(productFeatures, Integer.class);
            if (CollectionUtils.isNotEmpty(featuresList)) {
                List<ConfDictionaryDO> confDictionaryDOS = dictionaryService.listByIds(featuresList);
                if (CollectionUtils.isNotEmpty(confDictionaryDOS)) {
                    syncBasicInfoToErpModel.setIsFumigation(confDictionaryDOS.stream().map(ConfDictionaryDO::getDataName).collect(Collectors.toSet())
                            .contains(DictConstant.FUMIGATION_FEATURE)? DataStatusConstants.YES:
                            DataStatusConstants.NO);
                }
            }
        }
        return syncBasicInfoToErpModel;
    }


    /**
     * 添加申报价值
     * @param skuDO
     * @param basicInfo
     * @return
     */
    private SyncBasicInfoToErpModel addDeclaredValue(SkuDO skuDO,SyncBasicInfoToErpModel basicInfo) {
        GoodsSizeInfo goodsSizeInfo = goodsSizeInfoService.lambdaQuery().eq(GoodsSizeInfo::getSkuId, basicInfo.getSkuId()).one();
        if (null != goodsSizeInfo) {
            if (SkuTypeEnum.PART.type.equals(skuDO.getType())) {
                goodsSizeInfo.computeDeclared();
                basicInfo.setDeclaredValue(BigDecimal.valueOf(goodsSizeInfo.getDeclaredValue()));
            }
        }
        return basicInfo;
    }


    /**
     * 单个sku添加包裝材质 商品属性有多个
     * @param basicInfo
     * @return
     */
    private SyncBasicInfoToErpModel addPackagingMaterial(SyncBasicInfoToErpModel basicInfo) {
        // 获取包装材质属性
        Long warehouseAttrId = getWarehouseAttrId();
        List<SkuAttributeValueDO> skuAttributeValueDOS = skuAttributeValueService.lambdaQuery().eq(SkuAttributeValueDO::getTargetId, basicInfo.getSkuId())
                .eq(SkuAttributeValueDO::getAttrId, warehouseAttrId).eq(SkuAttributeValueDO::getType, DataStatusConstants.GOODS_ATTRIBUTE_VALUE_TYPE_SKU).list();
        if (CollectionUtils.isEmpty(skuAttributeValueDOS)) {
            return basicInfo;
        }
        List<GoodsAttributeValueDO> goodsAttributeValueDOS =DataSynchronizedConvert.INSTANCE.convertGoodsAttributeList(skuAttributeValueDOS);
                // 获取skuIdList
        Set<Long> skuIdList = goodsAttributeValueDOS.stream().map(GoodsAttributeValueDO::getTargetId).collect(Collectors.toSet());
        if (skuIdList.size() > 1) {
            rpcClientService.sendDTalkMessage(StrUtil.format("包装材质同步失败 {} 不允许含有多个sku ",JSON.toJSONString(goodsAttributeValueDOS)));
            return basicInfo;
        }
        List<GoodsAttributeValueDO> packagingMaterialAttributeValue = goodsAttributeValueDOS.stream()
                .filter((goodsAttribute -> goodsAttribute.getAttrId().equals(warehouseAttrId)
                        && goodsAttribute.getType().equals(DataStatusConstants.GOODS_ATTRIBUTE_VALUE_TYPE_SKU)))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(packagingMaterialAttributeValue)) {
            if (packagingMaterialAttributeValue.size() > 1) {
                rpcClientService.sendDTalkMessage(StrUtil.format("包装材质同步失败 sku含有多个仓库属性 {}",JSON.toJSONString(packagingMaterialAttributeValue)));
                return basicInfo;
            }
            GoodsAttributeValueDO goodsAttributeValueDO = packagingMaterialAttributeValue.get(0);
            String valueName = goodsAttributeValueDO.getAttrValue();

            // 如果属性值为数字，则获取对应的属性名称
            if (NumberUtils.isNumber(valueName)) {
                AttributeValueDO attributeValueDO = attributeValueService.getById(Long.valueOf(valueName));
                if (null != attributeValueDO) {
                    valueName = attributeValueDO.getAttrValue();
                    // 根据属性值设置包装材质
                    switch (valueName) {
                        case DictConstant.PACKAGING_MATERIAL_CARTON:
                            basicInfo.setPackagingMaterial(ErpConstants.PACKAGING_MATERIAL_CARTON);
                            break;
                        case DictConstant.PACKAGING_MATERIAL_WOODEN_BOX:
                            basicInfo.setPackagingMaterial(ErpConstants.PACKAGING_MATERIAL_WOODEN_BOX);
                            break;
                        case DictConstant.PACKAGING_MATERIAL_IRREGULAR:
                            basicInfo.setPackagingMaterial(ErpConstants.PACKAGING_MATERIAL_IRREGULAR);
                            break;
                        default:
                            rpcClientService.sendDTalkMessage(StrUtil.format("包装材质{}未建立与ERP的映射关系",valueName));
                            return basicInfo;
                    }
                }else {
                    rpcClientService.sendDTalkMessage(StrUtil.format("未查询到属性值 {}",valueName));
                }
            }
        }
        return basicInfo;
    }


    private SyncBasicInfoToErpModel addGoodsImages(SyncBasicInfoToErpModel basicInfo) {

        List<SkuImageDO> skuImageDOList = skuImageService.lambdaQuery().in(SkuImageDO::getSkuId, basicInfo.getSkuId()).list();
        if (CollectionUtils.isNotEmpty(skuImageDOList)) {

            basicInfo.setImages(StringUtils.join(skuImageDOList.stream().map(SkuImageDO::getImageUrl).collect(Collectors.toList()), ","));
        }
        return basicInfo;
    }


    /**
     * 获取仓配属性id
     * @return
     */
    private Long getWarehouseAttrId() {
        ConfDictionaryDO dataDictionaryDO = dictionaryService.lambdaQuery()
                .eq(ConfDictionaryDO::getDataCode, DictConstant.ATTR_TYPE_DICT_CODE)
                .eq(ConfDictionaryDO::getDataName, DictConstant.ATTR_TYPE_NAME_WAREHOUSE)
                .one();
        if (dataDictionaryDO == null) {
            rpcClientService.sendDTalkMessage("同步商品属性失败 仓配字典不存在 ");
            return null;
        }
        AttributeItemDO attributeItemDO = attributeItemService.lambdaQuery()
                .eq(AttributeItemDO::getAttrName, DictConstant.PACKAGING_MATERIAL)
                .eq(AttributeItemDO::getAttrType, dataDictionaryDO.getId())
                .one();
        if (attributeItemDO == null) {
            rpcClientService.sendDTalkMessage("同步商品属性失败 包装材质属性不存在");
            return null;
        }
        return attributeItemDO.getAttrId();
    }


}
