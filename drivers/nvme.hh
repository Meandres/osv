#ifndef NVME_DRIVER_H
#define NVME_DRIVER_H

#include "drivers/driver.hh"
#include "drivers/pci-device.hh"
#include <osv/mempool.hh>
#include <osv/interrupt.hh>
#include <osv/msi.hh>

class nvme : public hw_driver {
public:
    explicit nvme(pci::device& dev);
    virtual ~nvme() {};

    virtual std::string get_name() const { return "nvme"; }

    virtual void dump_config(void);
    int transmit(struct mbuf* m_head);

    static hw_driver* probe(hw_device* dev);

private:
    void parse_pci_config();
    void stop();
    void enable_device();
    void do_version_handshake();
    void attach_queues_shared(struct ifnet* ifn, pci::bar *bar0);
    void fill_driver_shared();
    void allocate_interrupts();

    virtual void isr() {};

    void write_cmd(u32 cmd);
    u32 read_cmd(u32 cmd);

    void get_mac_address(u_int8_t *macaddr);
    void enable_interrupts();
    void disable_interrupts();

    //maintains the nvme instance number for multiple adapters
    static int _instance;
    int _id;
    struct ifnet* _ifn;

    pci::device& _dev;

    //Shared memory
    pci::bar *_bar0 = nullptr;
};
#endif
