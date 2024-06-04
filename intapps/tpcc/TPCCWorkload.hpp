#pragma onc, tide
#include "Schema.hpp"
// -------------------------------------------------------------------------------------
#include "RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <algorithm>
#include <vector>
using std::vector;
#include <iostream>
using std::cout;
using std::endl;
// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
struct TPCCWorkload
{
   static constexpr Integer OL_I_ID_C = 7911;  // in range [0, 8191]
   static constexpr Integer C_ID_C = 259;      // in range [0, 1023]
   // NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
   // be within [65, 119]
   static constexpr Integer C_LAST_LOAD_C = 157;  // in range [0, 255]
   static constexpr Integer C_LAST_RUN_C = 223;   // in range [0, 255]
   static constexpr Integer ITEMS_NO = 100000;    // 100K
   AdapterType<warehouse_t>& warehouse;
   AdapterType<district_t>& district;
   AdapterType<customer_t>& customer;
   AdapterType<customer_wdl_t>& customerwdl;
   AdapterType<history_t>& history;
   AdapterType<neworder_t>& neworder;
   AdapterType<order_t>& order;
   AdapterType<order_wdc_t>& order_wdc;
   AdapterType<orderline_t>& orderline;
   AdapterType<item_t>& item;
   AdapterType<stock_t>& stock;
   const bool order_wdc_index = true;
   const Integer warehouseCount;
   const Integer tpcc_remove;
   const bool manually_handle_isolation_anomalies;
   const bool cross_warehouses;
   // -------------------------------------------------------------------------------------
   Integer urandexcept(Integer low, Integer high, Integer v, u64 tid)
   {
      if (high <= low)
         return low;
      Integer r = rnd(high - low, tid) + low;
      if (r >= v)
         return r + 1;
      else
         return r;
   }

   template <int maxLength>
   Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr, u64 tid)
   {
      assert(maxLenStr <= maxLength);
      Integer len = rnd(maxLenStr - minLenStr + 1, tid) + minLenStr;
      Varchar<maxLength> result;
      for (Integer index = 0; index < len; index++) {
         Integer i = rnd(62, tid);
         if (i < 10)
            result.append(48 + i);
         else if (i < 36)
            result.append(64 - 10 + i);
         else
            result.append(96 - 36 + i);
      }
      return result;
   }

   Varchar<16> randomnstring(Integer minLenStr, Integer maxLenStr, u64 tid)
   {
      Integer len = rnd(maxLenStr - minLenStr + 1, tid) + minLenStr;
      Varchar<16> result;
      for (Integer i = 0; i < len; i++)
         result.append(48 + rnd(10, tid));
      return result;
   }

   Varchar<16> namePart(Integer id)
   {
      assert(id < 10);
      Varchar<16> data[] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
      return data[id];
   }

   Varchar<16> genName(Integer id) { return namePart((id / 100) % 10) || namePart((id / 10) % 10) || namePart(id % 10); }

   Numeric randomNumeric(Numeric min, Numeric max, u64 tid)
   {
      double range = (max - min);
      double div = RAND_MAX / range;
      return min + (RandomGenerator::getRandU64(tid) / div);
   }

   Varchar<9> randomzip(u64 tid)
   {
      Integer id = rnd(10000, tid);
      Varchar<9> result;
      result.append(48 + (id / 1000));
      result.append(48 + (id / 100) % 10);
      result.append(48 + (id / 10) % 10);
      result.append(48 + (id % 10));
      return result || Varchar<9>("11111");
   }

   Integer nurand(Integer a, Integer x, Integer y, u64 tid, Integer C = 42)
   {
      // TPC-C random is [a,b] inclusive
      // in standard: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
      // return (((rnd(a + 1) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
      return (((urand(0, a, tid) | urand(x, y, tid)) + C) % (y - x + 1)) + x;
      // incorrect: return (((rnd(a) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
   }

   inline Integer getItemID(u64 tid)
   {
      // OL_I_ID_C
      return nurand(8191, 1, ITEMS_NO, OL_I_ID_C, tid);
   }
   inline Integer getCustomerID(u64 tid)
   {
      // C_ID_C
      return nurand(1023, 1, 3000, C_ID_C, tid);
      // return urand(1, 3000);
   }
   inline Integer getNonUniformRandomLastNameForRun(u64 tid)
   {
      // C_LAST_RUN_C
      return nurand(255, 0, 999, C_LAST_RUN_C, tid);
   }
   inline Integer getNonUniformRandomLastNameForLoad(u64 tid)
   {
      // C_LAST_LOAD_C
      return nurand(255, 0, 999, C_LAST_LOAD_C, tid);
   }
   // -------------------------------------------------------------------------------------
   Timestamp currentTimestamp() { return 1; }
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   // run
   void newOrder(Integer w_id,
                 Integer d_id,
                 Integer c_id,
                 const vector<Integer>& lineNumbers,
                 const vector<Integer>& supwares,
                 const vector<Integer>& itemids,
                 const vector<Integer>& qtys,
                 Timestamp timestamp,
                 int tid)
   {
      Numeric w_tax = warehouse.lookupField({w_id}, &warehouse_t::w_tax, tid);
      Numeric c_discount = customer.lookupField({w_id, d_id, c_id}, &customer_t::c_discount, tid);
      Numeric d_tax;
      Integer o_id;

      district.update1(
          {w_id, d_id},
          [&](district_t& rec) {
             d_tax = rec.d_tax;
             o_id = rec.d_next_o_id++;
          }, tid);

      Numeric all_local = 1;
      for (Integer sw : supwares)
         if (sw != w_id)
            all_local = 0;
      Numeric cnt = lineNumbers.size();
      Integer carrier_id = 0; /*null*/
      order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local}, tid);
      if (order_wdc_index) {
         order_wdc.insert({w_id, d_id, c_id, o_id}, {}, tid);
      }
      neworder.insert({w_id, d_id, o_id}, {}, tid);

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
         stock.update1(
             {supwares[i], itemids[i]},
             [&](stock_t& rec) {
                auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                rec.s_remote_cnt += (supwares[i] != w_id);
                rec.s_order_cnt++;
                rec.s_ytd += qty;
             }, tid);
      }

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer lineNumber = lineNumbers[i];
         Integer supware = supwares[i];
         Integer itemid = itemids[i];
         Numeric qty = qtys[i];

         Numeric i_price = item.lookupField({itemid}, &item_t::i_price, tid);  // TODO: rollback on miss
         Varchar<24> s_dist;
         stock.lookup1({w_id, itemid}, [&](const stock_t& rec) {
            switch (d_id) {
               case 1:
                  s_dist = rec.s_dist_01;
                  break;
               case 2:
                  s_dist = rec.s_dist_02;
                  break;
               case 3:
                  s_dist = rec.s_dist_03;
                  break;
               case 4:
                  s_dist = rec.s_dist_04;
                  break;
               case 5:
                  s_dist = rec.s_dist_05;
                  break;
               case 6:
                  s_dist = rec.s_dist_06;
                  break;
               case 7:
                  s_dist = rec.s_dist_07;
                  break;
               case 8:
                  s_dist = rec.s_dist_08;
                  break;
               case 9:
                  s_dist = rec.s_dist_09;
                  break;
               case 10:
                  s_dist = rec.s_dist_10;
                  break;
               default:
                  exit(1);
                  throw;
            }
         }, tid);
         Numeric ol_amount = qty * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
         Timestamp ol_delivery_d = 0;  // NULL
         orderline.insert({w_id, d_id, o_id, lineNumber}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist}, tid);
         // TODO: i_data, s_data
      }
   }
   // -------------------------------------------------------------------------------------
   void newOrderRnd(Integer w_id, int tid)
   {
      Integer d_id = urand(1, 10, tid);
      Integer c_id = getCustomerID(tid);
      Integer ol_cnt = urand(5, 15, tid);

      vector<Integer> lineNumbers;
      lineNumbers.reserve(15);
      vector<Integer> supwares;
      supwares.reserve(15);
      vector<Integer> itemids;
      itemids.reserve(15);
      vector<Integer> qtys;
      qtys.reserve(15);
      for (Integer i = 1; i <= ol_cnt; i++) {
         Integer supware = w_id;
         if (cross_warehouses && urand(1, 100, tid) == 1)  // ATTN:remote transaction
            supware = urandexcept(1, warehouseCount, w_id, tid);
         Integer itemid = getItemID(tid);
         if (false && (i == ol_cnt) && (urand(1, 100, tid) == 1))  // invalid item => random
            itemid = 0;
         lineNumbers.push_back(i);
         supwares.push_back(supware);
         itemids.push_back(itemid);
         qtys.push_back(urand(1, 10, tid));
      }
      newOrder(w_id, d_id, c_id, lineNumbers, supwares, itemids, qtys, currentTimestamp(), tid);
   }
   // -------------------------------------------------------------------------------------
   void delivery(Integer w_id, Integer carrier_id, Timestamp datetime, int tid)
   {
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         Integer o_id = minInteger;
         neworder.scan(
             {w_id, d_id, minInteger},
             [&](const neworder_t::Key& key, const neworder_t&) {
                if (key.no_w_id == w_id && key.no_d_id == d_id) {
                   o_id = key.no_o_id;
                }
                return false;
             },
             [&]() { o_id = minInteger; }, tid);
         // -------------------------------------------------------------------------------------
         if (o_id == minInteger) {  // Should rarely happen
            cout << "WARNING: delivery tx skipped for warehouse = " << w_id << ", district = " << d_id << endl;
            continue;
         }
         // -------------------------------------------------------------------------------------
         if (tpcc_remove) {
            const auto ret = neworder.erase({w_id, d_id, o_id}, tid);
            assert(ret || manually_handle_isolation_anomalies);
         }
         // -------------------------------------------------------------------------------------
         Integer ol_cnt = minInteger, c_id;
         if (manually_handle_isolation_anomalies) {
            order.scan(
                {w_id, d_id, o_id},
                [&](const order_t::Key&, const order_t& rec) {
                   ol_cnt = rec.o_ol_cnt;
                   c_id = rec.o_c_id;
                   return false;
                },
                [&]() {}, tid);
            if (ol_cnt == minInteger)
               continue;
         } else {
            order.lookup1({w_id, d_id, o_id}, [&](const order_t& rec) {
               ol_cnt = rec.o_ol_cnt;
               c_id = rec.o_c_id;
            }, tid);
         }
         // -------------------------------------------------------------------------------------
         if (manually_handle_isolation_anomalies) {
            bool is_safe_to_continue = false;
            order.scan(
                {w_id, d_id, o_id},
                [&](const order_t::Key& key, const order_t& rec) {
                   if (key.o_w_id == w_id && key.o_d_id == d_id && key.o_id == o_id) {
                      is_safe_to_continue = true;
                      ol_cnt = rec.o_ol_cnt;
                      c_id = rec.o_c_id;
                   } else {
                      is_safe_to_continue = false;
                   }
                   return false;
                },
                [&]() { is_safe_to_continue = false; }, tid);
            if (!is_safe_to_continue)
               continue;
         }
         // -------------------------------------------------------------------------------------
         order.update1(
             {w_id, d_id, o_id}, [&](order_t& rec) { rec.o_carrier_id = carrier_id; }, tid);
         // -------------------------------------------------------------------------------------
         if (manually_handle_isolation_anomalies) {
            // First check if all orderlines have been inserted, a hack because of the missing transaction and concurrency control
            bool is_safe_to_continue = false;
            orderline.scan(
                {w_id, d_id, o_id, ol_cnt},
                [&](const orderline_t::Key& key, const orderline_t&) {
                   if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id && key.ol_number == ol_cnt) {
                      is_safe_to_continue = true;
                   } else {
                      is_safe_to_continue = false;
                   }
                   return false;
                },
                [&]() { is_safe_to_continue = false; }, tid);
            if (!is_safe_to_continue) {
               continue;
            }
         }
         // -------------------------------------------------------------------------------------
         Numeric ol_total = 0;
         for (Integer ol_number = 1; ol_number <= ol_cnt; ol_number++) {
            orderline.update1(
                {w_id, d_id, o_id, ol_number},
                [&](orderline_t& rec) {
                   ol_total += rec.ol_amount;
                   rec.ol_delivery_d = datetime;
                }, tid);
         }
         customer.update1(
             {w_id, d_id, c_id},
             [&](customer_t& rec) {
                rec.c_balance += ol_total;
                rec.c_delivery_cnt++;
             }, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void deliveryRnd(Integer w_id, int tid)
   {
      Integer carrier_id = urand(1, 10, tid);
      delivery(w_id, carrier_id, currentTimestamp(), tid);
   }
   // -------------------------------------------------------------------------------------
   void stockLevel(Integer w_id, Integer d_id, Integer threshold, int tid)
   {
      Integer o_id = district.lookupField({w_id, d_id}, &district_t::d_next_o_id, tid);

      //"SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM orderline, stock WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND
      // S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?"

      /*
       * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf P 116
       * EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
    FROM order_line, stock
    WHERE ol_w_id=:w_id AND
    ol_d_id=:d_id AND ol_o_id<:o_id AND
    ol_o_id>=:o_id-20 AND s_w_id=:w_id AND
    s_i_id=ol_i_id AND s_quantity < :threshold;
       */
      vector<Integer> items;
      items.reserve(100);
      Integer min_ol_o_id = o_id - 20;
      orderline.scan(
          {w_id, d_id, min_ol_o_id, minInteger},
          [&](const orderline_t::Key& key, const orderline_t& rec) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id < o_id && key.ol_o_id >= min_ol_o_id) {
                items.push_back(rec.ol_i_id);
                return true;
             }
             return false;
          },
          [&]() { items.clear(); }, tid);
      std::sort(items.begin(), items.end());
      auto last = std::unique(items.begin(), items.end());
      items.erase(last, items.end());
      unsigned count = 0;
      for (Integer i_id : items) {
         auto res_s_quantity = stock.lookupField({w_id, i_id}, &stock_t::s_quantity, tid);
         count += res_s_quantity < threshold;
      }
   }
   // -------------------------------------------------------------------------------------
   void stockLevelRnd(Integer w_id, int tid) { stockLevel(w_id, urand(1, 10, tid), urand(10, 20, tid), tid); }
   // -------------------------------------------------------------------------------------
   void orderStatusId(Integer w_id, Integer d_id, Integer c_id, int tid)
   {
      Varchar<16> c_first;
      Varchar<2> c_middle;
      Varchar<16> c_last;
      Numeric c_balance;
      customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {
         c_first = rec.c_first;
         c_middle = rec.c_middle;
         c_last = rec.c_last;
         c_balance = rec.c_balance;
      }, tid);

      Integer o_id = -1;
      // -------------------------------------------------------------------------------------
      // latest order id desc
      if (order_wdc_index) {
         order_wdc.scanDesc(
             {w_id, d_id, c_id, std::numeric_limits<Integer>::max()},
             [&](const order_wdc_t::Key& key, const order_wdc_t&) {
                assert(key.o_w_id == w_id);
                assert(key.o_d_id == d_id);
                assert(key.o_c_id == c_id);
                o_id = key.o_id;
                return false;
             },
             [] {}, tid);
      } else {
         order.scanDesc(
             {w_id, d_id, std::numeric_limits<Integer>::max()},
             [&](const order_t::Key& key, const order_t& rec) {
                if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id) {
                   o_id = key.o_id;
                   return false;
                }
                return true;
             },
             [&]() {}, tid);
      }
      if (o_id == -1)
         return;
      // -------------------------------------------------------------------------------------
      Timestamp o_entry_d;
      Integer o_carrier_id;

      order.lookup1({w_id, d_id, o_id}, [&](const order_t& rec) {
         o_entry_d = rec.o_entry_d;
         o_carrier_id = rec.o_carrier_id;
      }, tid);
      Integer ol_i_id;
      Integer ol_supply_w_id;
      Timestamp ol_delivery_d;
      Numeric ol_quantity;
      Numeric ol_amount;
      {
         // AAA: expensive
         orderline.scan(
             {w_id, d_id, o_id, minInteger},
             [&](const orderline_t::Key& key, const orderline_t& rec) {
                if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
                   ol_i_id = rec.ol_i_id;
                   ol_supply_w_id = rec.ol_supply_w_id;
                   ol_delivery_d = rec.ol_delivery_d;
                   ol_quantity = rec.ol_quantity;
                   ol_amount = rec.ol_amount;
                   return true;
                }
                return false;
             },
             [&]() {
                // NOTHING
             }, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void orderStatusName(Integer w_id, Integer d_id, Varchar<16> c_last, int tid)
   {
      vector<Integer> ids;
      customerwdl.scan(
          {w_id, d_id, c_last, {}},
          [&](const customer_wdl_t::Key& key, const customer_wdl_t& rec) {
             if (key.c_w_id == w_id && key.c_d_id == d_id && key.c_last == c_last) {
                ids.push_back(rec.c_id);
                return true;
             }
             return false;
          },
          [&]() { ids.clear(); }, tid);
      unsigned c_count = ids.size();
      if (c_count == 0)
         return;  // TODO: rollback
      unsigned index = c_count / 2;
      if ((c_count % 2) == 0)
         index -= 1;
      Integer c_id = ids[index];

      Integer o_id = -1;
      // latest order id desc
      if (order_wdc_index) {
         order_wdc.scanDesc(
             {w_id, d_id, c_id, std::numeric_limits<Integer>::max()},
             [&](const order_wdc_t::Key& key, const order_wdc_t&) {
                if (key.o_w_id == w_id && key.o_d_id == d_id && key.o_c_id == c_id)
                   o_id = key.o_id;
                return false;
             },
             [] {}, tid);
      } else {
         order.scanDesc(
             {w_id, d_id, std::numeric_limits<Integer>::max()},
             [&](const order_t::Key& key, const order_t& rec) {
                if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id)
                   o_id = key.o_id;
                return false;
             },
             [&]() {}, tid);
      }
      if (o_id == -1)
         return;
      // -------------------------------------------------------------------------------------
      Timestamp ol_delivery_d;
      orderline.scan(
          {w_id, d_id, o_id, minInteger},
          [&](const orderline_t::Key& key, const orderline_t& rec) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
                ol_delivery_d = rec.ol_delivery_d;
                return true;
             }
             return false;
          },
          []() {
             // NOTHING
          }, tid);
   }
   // -------------------------------------------------------------------------------------

   void orderStatusRnd(Integer w_id, int tid)
   {
      Integer d_id = urand(1, 10, tid);
      if (urand(1, 100, tid) <= 40) {
         orderStatusId(w_id, d_id, getCustomerID(tid), tid);
      } else {
         orderStatusName(w_id, d_id, genName(getNonUniformRandomLastNameForRun(tid)), tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void paymentById(Integer w_id, Integer d_id, Integer c_w_id, Integer c_d_id, Integer c_id, Timestamp h_date, Numeric h_amount, Timestamp datetime, uint16_t workerThreadId, uint32_t *tpcchistorycounter, int tid)
   {
      Varchar<10> w_name;
      Varchar<20> w_street_1;
      Varchar<20> w_street_2;
      Varchar<20> w_city;
      Varchar<2> w_state;
      Varchar<9> w_zip;
      Numeric w_ytd;
      warehouse.lookup1({w_id}, [&](const warehouse_t& rec) {
         w_name = rec.w_name;
         w_street_1 = rec.w_street_1;
         w_street_2 = rec.w_street_2;
         w_city = rec.w_city;
         w_state = rec.w_state;
         w_zip = rec.w_zip;
         w_ytd = rec.w_ytd;
      }, tid);
      // -------------------------------------------------------------------------------------
      warehouse.update1(
          {w_id}, [&](warehouse_t& rec) { rec.w_ytd += h_amount; }, tid);
      Varchar<10> d_name;
      Varchar<20> d_street_1;
      Varchar<20> d_street_2;
      Varchar<20> d_city;
      Varchar<2> d_state;
      Varchar<9> d_zip;
      Numeric d_ytd;
      district.lookup1({w_id, d_id}, [&](const district_t& rec) {
         d_name = rec.d_name;
         d_street_1 = rec.d_street_1;
         d_street_2 = rec.d_street_2;
         d_city = rec.d_city;
         d_state = rec.d_state;
         d_zip = rec.d_zip;
         d_ytd = rec.d_ytd;
      }, tid);
      district.update1(
          {w_id, d_id}, [&](district_t& rec) { rec.d_ytd += h_amount; }, tid);

      Varchar<500> c_data;
      Varchar<2> c_credit;
      Numeric c_balance;
      Numeric c_ytd_payment;
      Numeric c_payment_cnt;
      customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {
         c_data = rec.c_data;
         c_credit = rec.c_credit;
         c_balance = rec.c_balance;
         c_ytd_payment = rec.c_ytd_payment;
         c_payment_cnt = rec.c_payment_cnt;
      }, tid);
      Numeric c_new_balance = c_balance - h_amount;
      Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
      Numeric c_new_payment_cnt = c_payment_cnt + 1;

      if (c_credit == "BC") {
         Varchar<500> c_new_data;
         auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount,
                                  h_date, w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
         c_new_data.length = numChars;
         if (c_new_data.length > 500)
            c_new_data.length = 500;
         // -------------------------------------------------------------------------------------
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_data = c_new_data;
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             }, tid);
      } else {
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             }, tid);
      }

      Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
      Integer t_id = (Integer)workerThreadId;
      Integer h_id = (Integer)(*tpcchistorycounter)++;
      history.insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data}, tid);
   }
   // -------------------------------------------------------------------------------------
   void paymentByName(Integer w_id,
                      Integer d_id,
                      Integer c_w_id,
                      Integer c_d_id,
                      Varchar<16> c_last,
                      Timestamp h_date,
                      Numeric h_amount,
                      Timestamp datetime,
                      uint16_t workerThreadId,
                      uint32_t *tpcchistorycounter,
                      int tid)
   {
      Varchar<10> w_name;
      Varchar<20> w_street_1;
      Varchar<20> w_street_2;
      Varchar<20> w_city;
      Varchar<2> w_state;
      Varchar<9> w_zip;
      Numeric w_ytd;
      warehouse.lookup1({w_id}, [&](const warehouse_t& rec) {
         w_name = rec.w_name;
         w_street_1 = rec.w_street_1;
         w_street_2 = rec.w_street_2;
         w_city = rec.w_city;
         w_state = rec.w_state;
         w_zip = rec.w_zip;
         w_ytd = rec.w_ytd;
      }, tid);
      // -------------------------------------------------------------------------------------
      warehouse.update1(
          {w_id}, [&](warehouse_t& rec) { rec.w_ytd += h_amount; }, tid);
      // -------------------------------------------------------------------------------------
      Varchar<10> d_name;
      Varchar<20> d_street_1;
      Varchar<20> d_street_2;
      Varchar<20> d_city;
      Varchar<2> d_state;
      Varchar<9> d_zip;
      Numeric d_ytd;
      district.lookup1({w_id, d_id}, [&](const district_t& rec) {
         d_name = rec.d_name;
         d_street_1 = rec.d_street_1;
         d_street_2 = rec.d_street_2;
         d_city = rec.d_city;
         d_state = rec.d_state;
         d_zip = rec.d_zip;
         d_ytd = rec.d_ytd;
      }, tid);
      district.update1(
          {w_id, d_id}, [&](district_t& rec) { rec.d_ytd += h_amount; }, tid);

      // Get customer id by name
      vector<Integer> ids;
      customerwdl.scan(
          {c_w_id, c_d_id, c_last, {}},
          [&](const customer_wdl_t::Key& key, const customer_wdl_t& rec) {
             if (key.c_w_id == c_w_id && key.c_d_id == c_d_id && key.c_last == c_last) {
                ids.push_back(rec.c_id);
                return true;
             }
             return false;
          },
          [&]() { ids.clear(); }, tid);
      unsigned c_count = ids.size();
      if (c_count == 0)
         return;  // TODO: rollback
      unsigned index = c_count / 2;
      if ((c_count % 2) == 0)
         index -= 1;
      Integer c_id = ids[index];

      Varchar<500> c_data;
      Varchar<2> c_credit;
      Numeric c_balance;
      Numeric c_ytd_payment;
      Numeric c_payment_cnt;
      customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {
         c_data = rec.c_data;
         c_credit = rec.c_credit;
         c_balance = rec.c_balance;
         c_ytd_payment = rec.c_ytd_payment;
         c_payment_cnt = rec.c_payment_cnt;
      }, tid);
      Numeric c_new_balance = c_balance - h_amount;
      Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
      Numeric c_new_payment_cnt = c_payment_cnt + 1;

      if (c_credit == "BC") {
         Varchar<500> c_new_data;
         auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount,
                                  h_date, w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
         c_new_data.length = numChars;
         if (c_new_data.length > 500)
            c_new_data.length = 500;
         // -------------------------------------------------------------------------------------
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_data = c_new_data;
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             }, tid);
      } else {
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             }, tid);
      }

      Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
      Integer t_id = (Integer)workerThreadId;
      Integer h_id = (Integer)(*tpcchistorycounter)++;
      history.insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data}, tid);
   }
   // -------------------------------------------------------------------------------------
   void paymentRnd(Integer w_id, uint16_t workerThreadId, uint32_t *tpcchistorycounter, int tid)
   {
      Integer d_id = urand(1, 10, tid);
      Integer c_w_id = w_id;
      Integer c_d_id = d_id;
      if (cross_warehouses && urand(1, 100, tid) > 85) {  // ATTN: cross warehouses
         c_w_id = urandexcept(1, warehouseCount, w_id, tid);
         c_d_id = urand(1, 10, tid);
      }
      Numeric h_amount = randomNumeric(1.00, 5000.00, tid);
      Timestamp h_date = currentTimestamp();

      if (urand(1, 100, tid) <= 60) {
         paymentByName(w_id, d_id, c_w_id, c_d_id, genName(getNonUniformRandomLastNameForRun(tid)), h_date, h_amount, currentTimestamp(), workerThreadId, tpcchistorycounter, tid);
      } else {
         paymentById(w_id, d_id, c_w_id, c_d_id, getCustomerID(tid), h_date, h_amount, currentTimestamp(), workerThreadId, tpcchistorycounter, tid);
      }
   }
   // -------------------------------------------------------------------------------------
  public:
   TPCCWorkload(AdapterType<warehouse_t>& w,
                AdapterType<district_t>& d,
                AdapterType<customer_t>& customer,
                AdapterType<customer_wdl_t>& customerwdl,
                AdapterType<history_t>& history,
                AdapterType<neworder_t>& neworder,
                AdapterType<order_t>& order,
                AdapterType<order_wdc_t>& order_wdc,
                AdapterType<orderline_t>& orderline,
                AdapterType<item_t>& item,
                AdapterType<stock_t>& stock,
                bool order_wdc_index,
                Integer warehouse_count,
                bool tpcc_remove,
                bool manually_handle_isolation_anomalies = true,
                bool cross_warehouses = true)
       : warehouse(w),
         district(d),
         customer(customer),
         customerwdl(customerwdl),
         history(history),
         neworder(neworder),
         order(order),
         order_wdc(order_wdc),
         orderline(orderline),
         item(item),
         stock(stock),
         order_wdc_index(order_wdc_index),
         warehouseCount(warehouse_count),
         tpcc_remove(tpcc_remove),
         manually_handle_isolation_anomalies(manually_handle_isolation_anomalies),
         cross_warehouses(cross_warehouses)
   {
   }
   // -------------------------------------------------------------------------------------
   // [0, n)
   Integer rnd(Integer n, u64 tid) { return RandomGenerator::getRand(0, n, tid); }
   // [fromId, toId]
   Integer randomId(Integer fromId, Integer toId, u64 tid) { return RandomGenerator::getRand(fromId, toId + 1, tid); }
   // [low, high]
   Integer urand(Integer low, Integer high, u64 tid) { return rnd(high - low + 1, tid) + low; }
   // -------------------------------------------------------------------------------------
   void loadStock(Integer w_id, int tid)
   {
      for (Integer i = 0; i < ITEMS_NO; i++) {
         Varchar<50> s_data = randomastring<50>(25, 50, tid);
         if (rnd(10, tid) == 0) {
            s_data.length = rnd(s_data.length - 8, tid);
            s_data = s_data || Varchar<10>("ORIGINAL");
         }
         stock.insert({w_id, i + 1}, {randomNumeric(10, 100, tid), randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid),
                                      randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid),
                                      randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid), randomastring<24>(24, 24, tid), 0, 0, 0, s_data}, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void loadDistrinct(Integer w_id, int tid)
   {
      for (Integer i = 1; i < 11; i++) {
         district.insert({w_id, i}, {randomastring<10>(6, 10, tid), randomastring<20>(10, 20, tid), randomastring<20>(10, 20, tid), randomastring<20>(10, 20, tid),
                                     randomastring<2>(2, 2, tid), randomzip(tid), randomNumeric(0.0000, 0.2000, tid), 3000000, 3001}, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void loadCustomer(Integer w_id, Integer d_id, uint16_t workerThreadId, uint32_t *tpcchistorycounter, int tid)
   {
      Timestamp now = currentTimestamp();
      for (Integer i = 0; i < 3000; i++) {
         Varchar<16> c_last;
         if (i < 1000)
            c_last = genName(i);
         else
            c_last = genName(getNonUniformRandomLastNameForLoad(tid));
         Varchar<16> c_first = randomastring<16>(8, 16, tid);
         Varchar<2> c_credit(rnd(10, tid) ? "GC" : "BC");
         customer.insert({w_id, d_id, i + 1}, {c_first, "OE", c_last, randomastring<20>(10, 20, tid), randomastring<20>(10, 20, tid), randomastring<20>(10, 20, tid),
                                               randomastring<2>(2, 2, tid), randomzip(tid), randomnstring(16, 16, tid), now, c_credit, 50000.00,
                                               randomNumeric(0.0000, 0.5000, tid), -10.00, 1, 0, 0, randomastring<500>(300, 500, tid)}, tid);
         customerwdl.insert({w_id, d_id, c_last, c_first}, {i + 1}, tid);
         Integer t_id = (Integer)workerThreadId;
         Integer h_id = (Integer)(*tpcchistorycounter)++;
         history.insert({t_id, h_id}, {i + 1, d_id, w_id, d_id, w_id, now, 10.00, randomastring<24>(12, 24, tid)}, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void loadOrders(Integer w_id, Integer d_id, int tid)
   {
      Timestamp now = currentTimestamp();
      vector<Integer> c_ids;
      for (Integer i = 1; i <= 3000; i++)
         c_ids.push_back(i);
      for (Integer i=3000; i>=1 ;i--)
         std::swap(c_ids[urand(0, i, tid)], c_ids[i]);
      Integer o_id = 1;
      for (Integer o_c_id : c_ids) {
         Integer o_carrier_id = (o_id < 2101) ? rnd(10, tid) + 1 : 0;
         Numeric o_ol_cnt = rnd(10, tid) + 5;

         order.insert({w_id, d_id, o_id}, {o_c_id, now, o_carrier_id, o_ol_cnt, 1}, tid);
         if (order_wdc_index) {
            order_wdc.insert({w_id, d_id, o_c_id, o_id}, {}, tid);
         }

         for (Integer ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            Timestamp ol_delivery_d = 0;
            if (o_id < 2101)
               ol_delivery_d = now;
            Numeric ol_amount = (o_id < 2101) ? 0 : randomNumeric(0.01, 9999.99, tid);
            const Integer ol_i_id = rnd(ITEMS_NO, tid) + 1;
            orderline.insert({w_id, d_id, o_id, ol_number}, {ol_i_id, w_id, ol_delivery_d, 5, ol_amount, randomastring<24>(24, 24, tid)}, tid);
         }
         o_id++;
      }

      for (Integer i = 2100; i <= 3000; i++)
         neworder.insert({w_id, d_id, i}, {}, tid);
   }
   // -------------------------------------------------------------------------------------
   void loadItem(int tid)
   {
      for (Integer i = 1; i <= ITEMS_NO; i++) {
         Varchar<50> i_data = randomastring<50>(25, 50, tid);
         if (rnd(10, tid) == 0) {
            i_data.length = rnd(i_data.length - 8, tid);
            i_data = i_data || Varchar<10>("ORIGINAL");
         }
         item.insert({i}, {randomId(1, 10000, tid), randomastring<24>(14, 24, tid), randomNumeric(1.00, 100.00, tid), i_data}, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   void loadWarehouse(int tid)
   {
      for (Integer i = 0; i < warehouseCount; i++) {
         warehouse.insert({i + 1}, {randomastring<10>(6, 10, tid), randomastring<20>(10, 20, tid), randomastring<20>(10, 20, tid), randomastring<20>(10, 20, tid),
                                    randomastring<2>(2, 2, tid), randomzip(tid), randomNumeric(0.1000, 0.2000, tid), 3000000}, tid);
      }
   }
   // -------------------------------------------------------------------------------------
   int tx(Integer w_id, uint16_t workerThreadId, uint32_t *tpcchistorycounter, int tid)
   {
      // micro-optimized version of weighted distribution
      u64 rnd = RandomGenerator::getRand(0, 10000, tid);
      if (rnd < 4300) {
         paymentRnd(w_id, workerThreadId, tpcchistorycounter, tid);
         return 0;
      }
      rnd -= 4300;
      if (rnd < 400) {
         orderStatusRnd(w_id, tid);
         return 1;
      }
      rnd -= 400;
      if (rnd < 400) {
         deliveryRnd(w_id, tid);
         return 2;
      }
      rnd -= 400;
      if (rnd < 400) {
         stockLevelRnd(w_id, tid);
         return 3;
      }
      rnd -= 400;
      newOrderRnd(w_id, tid);
      return 4;
   }
};
